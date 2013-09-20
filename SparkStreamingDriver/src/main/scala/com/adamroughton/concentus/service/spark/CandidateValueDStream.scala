package com.adamroughton.concentus.service.spark

import spark.streaming.{StreamingContext, DStream, Milliseconds, Time, Duration}
import spark.streaming.StreamingContext._
import spark.streaming.dstream.{NetworkInputDStream, NetworkReceiver}
import spark.{RDD, SparkEnv, Logging}
import spark.storage.StorageLevel
import spark.storage.StorageLevel._
import spark.streaming.receivers.{ActorReceiver, Receiver}
import spark.Utils
import akka.actor.{Props, Actor, ActorRef}
import akka.pattern.ask
import akka.dispatch.Await
import akka.util.duration._
import scala.collection.mutable.Queue
import scala.collection.mutable.ArrayBuffer
import scala.collection.immutable.Seq
import scala.collection.JavaConversions._
import scala.util.Random
import scala.math._
import scala.annotation.tailrec
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicBoolean
import java.net.InetAddress
import java.net.NetworkInterface
import com.adamroughton.concentus.data.model.kryo.CandidateValue
import com.adamroughton.concentus.actioncollector.{ActionCollectorService, TickDelegate}
import com.adamroughton.concentus.ConcentusHandle
import com.adamroughton.concentus.config.Configuration
import com.adamroughton.concentus.DefaultClock
import com.adamroughton.concentus.metric.NullMetricContext
import com.adamroughton.concentus.data.ResizingBuffer
import com.adamroughton.concentus.actioncollector.TickDelegate
import com.adamroughton.concentus.ConcentusExecutableOperations
import com.adamroughton.concentus.cluster.worker.ServiceContainer
import com.adamroughton.concentus.data.cluster.kryo.ServiceState
import com.adamroughton.concentus.cluster.worker.ClusterService
import com.adamroughton.concentus.metric.MetricContext
import com.adamroughton.concentus.actioncollector.ActionCollectorService.ActionCollectorServiceDeployment
import com.adamroughton.concentus.cluster.worker.ServiceContext
import com.adamroughton.concentus.ComponentResolver
import java.util.Collections
import com.adamroughton.concentus.cluster.worker.ClusterHandle
import java.util.UUID
import com.adamroughton.concentus.util.Container
import com.adamroughton.concentus.cluster.ClusterHandleSettings

private case class RegisterTickReceiver(streamId: Int, receiverActor: ActorRef, objId: String)
private case class Tick(time: Long)
private case object TickAck
private case class Initialize(lastTick: Long, tickDuration: Long)

class CandidateValueDStream(@transient ssc_ : StreamingContext,
    actionCollectorPort: Int,
    actionCollectorRecvBufferLength: Int,
    actionCollectorSendBufferLength: Int, 
    id: Int, 
    zooKeeperAddress: String, 
    zooKeeperAppRoot: String,
    resolver: ComponentResolver[_ <: ResizingBuffer]) 
		extends NetworkInputDStream[CandidateValue](ssc_) {

	private var lastTickTime = 0l
	val timeout = 2.seconds
	val env = SparkEnv.get

	def getReceiver() = new ActionReceiver(actionCollectorPort, actionCollectorRecvBufferLength, 
	    actionCollectorSendBufferLength, MEMORY_ONLY, id, zooKeeperAddress, zooKeeperAppRoot, resolver)
	
	lazy private val tickManager = env.actorSystem.actorOf(Props(
		new TickManagerActor(zeroTime.milliseconds, graph.batchDuration)), "TickManagerActor-" + id)
	
	override def start() {
		super.start
		tickManager
	}
	
	override def stop() {
		env.actorSystem.stop(tickManager)
	}

    override def compute(validTime: Time): Option[RDD[CandidateValue]] = {
		val tickTime = validTime.milliseconds
		if (tickTime > lastTickTime) {
			val future = tickManager.ask(Tick(tickTime))(timeout)
			Await.result(future, timeout)
			lastTickTime = tickTime
		}
		super.compute(validTime)
    }

	private class TickManagerActor(zeroTime: Long, tickDuration: Duration) extends Actor {
		
		private var receiverActor: ActorRef = null
		private var pendingRes: ActorRef = null
		private var lastTick: Long = zeroTime
		
		def receive = {
			case RegisterTickReceiver(streamId: Int, receiverActor: ActorRef, objId: String) => {
				val addressFunc = (ref: ActorRef) => ref.path.elements.reduce((s1, s2) => s1 + "/" + s2)
				this.receiverActor = receiverActor
				sender ! Initialize(lastTick, tickDuration.milliseconds)
			}
			case TickAck => {
				if (pendingRes != null) {
					pendingRes ! true
					pendingRes = null
				}
			}
			case Tick(time: Long) => {
				if (receiverActor != null) {
					pendingRes = sender
					receiverActor ! Tick(time)
				} else {
					sender ! true
				}
				lastTick = time
			}
		}
	}

}

class ActionReceiver(
    actionCollectorPort: Int,
    actionCollectorRecvBufferLength: Int,
    actionCollectorSendBufferLength: Int,
    storageLevel: StorageLevel, 
	id: Int, 
	zooKeeperAddress: String,
	zooKeeperAppRoot: String,
	resolver: ComponentResolver[_ <: ResizingBuffer]) 
		extends NetworkReceiver[CandidateValue] with TickDelegate {

	private[this] case class TickProcessingDone(time: Long)
	
	val timeout = 5.seconds
	
	lazy private val tickActor = env.actorSystem.actorOf(
	    	Props(new TickReceiverActor(id, zooKeeperAddress, zooKeeperAppRoot)), "ActionProcessor-" + streamId)
	
	protected def onStart() {
		tickActor
	}
	
	protected def onStop() {
		env.actorSystem.stop(tickActor)
	}
	
	def onTick(time: Long, candidateValuesIterator: java.util.Iterator[CandidateValue]) = {
		val candidateValues = new ArrayBuffer[CandidateValue]
		candidateValuesIterator.copyToBuffer(candidateValues)
		
		pushBlock("block-" + streamId + "-" + time, candidateValues, null, storageLevel)
		tickActor ! TickProcessingDone(time)
	}
	
	private class TickReceiverActor(id: Int,  
				zooKeeperAddress: String,
				zooKeeperAppRoot: String) extends Actor {
		val ip = System.getProperty("spark.driver.host", "localhost")
		val port = System.getProperty("spark.driver.port", "7077").toInt
		val url = "akka://spark@%s:%s/user/TickManagerActor-%d".format(ip, port, streamId)
		val tickManager = env.actorSystem.actorFor(url)
		val timeout = 5.seconds
	
		private var tickDuration = 0l
		private var lastTick = 0l
		private var actionCollectorService: ActionCollectorService[_ <: ResizingBuffer] = null
		private var actionCollectorServiceContainer: ServiceContainer[ServiceState] = null

		override def preStart() {
			val future = tickManager.ask(RegisterTickReceiver(streamId, self, this.toString))(timeout)
			val initMsg = Await.result(future, timeout).asInstanceOf[Initialize]
			this.lastTick = initMsg.lastTick
			this.tickDuration = initMsg.tickDuration
			val (service, container) = createActionCollectorService(lastTick, tickDuration)
			actionCollectorService = service
			actionCollectorServiceContainer = container
			actionCollectorServiceContainer.start()
		}
		
		override def postStop() {
		    this.actionCollectorServiceContainer.close()
		}

		private def createActionCollectorService(startTime: Long, tickDuration: Long): 
				(ActionCollectorService[_ <: ResizingBuffer], ServiceContainer[ServiceState]) = {
			val clock = new DefaultClock
			
			// get the worker's IP address
			val sparkIpProp = System.getenv("SPARK_LOCAL_IP")
			if (sparkIpProp == null)
			  throw new RuntimeException("The ip address was not specified for this worker")
			val receiverAddress = InetAddress.getByName(sparkIpProp)
			
			val serviceRef = new Container[ActionCollectorService[_ <: ResizingBuffer]]
			val actionCollectorDeployment = new ActionCollectorServiceDeployment(actionCollectorPort, 
			    actionCollectorRecvBufferLength, actionCollectorSendBufferLength, ActionReceiver.this, startTime, tickDuration) {
			   
			  override def createService[TBuffer <: ResizingBuffer](serviceId: Int, context: 
			      ServiceContext[ServiceState], handle: ConcentusHandle, metricContext: MetricContext,
			      resolver: ComponentResolver[TBuffer]): ClusterService[ServiceState] = {
					  val service = super.createService(serviceId, context, handle, metricContext, resolver)
							  .asInstanceOf[ActionCollectorService[_ <: ResizingBuffer]]
					  serviceRef.set(service) 
			    	  service
			  }
			  
			}
			
			val concentusHandle = new ConcentusHandle(new DefaultClock(), receiverAddress, zooKeeperAddress, Collections.emptySet())
			val clusterHandleSettings = new ClusterHandleSettings(zooKeeperAddress, zooKeeperAppRoot, UUID.randomUUID, concentusHandle)
			
			val serviceContainer = new ServiceContainer(clusterHandleSettings, concentusHandle, 
			    actionCollectorDeployment, resolver)
			(serviceRef.get(), serviceContainer)
		}
		
		override def receive() = {
			case Tick(time: Long) => {
				actionCollectorService.tick(time)
			}
			case TickProcessingDone(time: Long) => {
			   sender ! TickAck
			}
		}
		
	}

}