package com.adamroughton.concentus.service.spark

import java.io.IOException
import java.io.ObjectInputStream
import java.io.ObjectOutputStream
import java.net.InetAddress
import java.util.Collections
import scala.collection.JavaConversions.asScalaIterator
import scala.collection.mutable.ArrayBuffer
import com.adamroughton.concentus.ComponentResolver
import com.adamroughton.concentus.ConcentusHandle
import com.adamroughton.concentus.DefaultClock
import com.adamroughton.concentus.actioncollector.ActionCollectorService
import com.adamroughton.concentus.actioncollector.ActionCollectorService.ActionCollectorServiceDeployment
import com.adamroughton.concentus.actioncollector.TickDelegate
import com.adamroughton.concentus.cluster.ClusterHandleSettings
import com.adamroughton.concentus.cluster.worker.ClusterService
import com.adamroughton.concentus.cluster.worker.ServiceContainer
import com.adamroughton.concentus.cluster.worker.ServiceContext
import com.adamroughton.concentus.cluster.worker.StateData
import com.adamroughton.concentus.data.ResizingBuffer
import com.adamroughton.concentus.data.cluster.kryo.ServiceState
import com.adamroughton.concentus.data.model.kryo.CandidateValue
import com.adamroughton.concentus.metric.MetricContext
import com.adamroughton.concentus.util.Container
import com.adamroughton.concentus.util.Util
import akka.actor.Actor
import akka.actor.ActorRef
import akka.actor.Props
import akka.actor.actorRef2Scala
import akka.dispatch.Await
import akka.pattern.ask
import akka.util.Timeout
import akka.util.duration.intToDurationInt
import spark.RDD
import spark.SparkEnv
import spark.storage.StorageLevel
import spark.storage.StorageLevel.MEMORY_ONLY
import spark.streaming.Duration
import spark.streaming.StreamingContext
import spark.streaming.Time
import spark.streaming.dstream.NetworkInputDStream
import spark.streaming.dstream.NetworkReceiver
import akka.remote.RemoteActorRefProvider
import akka.actor.{ActorSystem, ActorSystemImpl, Actor, ActorRef, Props}
import com.typesafe.config.ConfigFactory
import scala.collection.JavaConversions._
import java.net.NetworkInterface
import java.net.Inet4Address
import java.net.URLClassLoader
import com.adamroughton.concentus.util.ParentLastURLClassLoader
import com.adamroughton.concentus.cluster.worker.ServiceDeployment
import com.adamroughton.concentus.actioncollector.TickDriven
import spark.Logging
import com.adamroughton.concentus.cluster.worker.ServiceContainerImpl
import java.io.Closeable
import java.util.concurrent.atomic.AtomicReference
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap
import java.util.concurrent.TimeoutException

private case object DoInit
private case class ActionCollectorReady(actionCollector: TickDriven)
private case class RegisterTickReceiver(streamId: Int, receiverActor: ActorRef, objId: String)
private case class Tick(time: Long)
private case class TickAck(time: Long)
private case class Initialize(lastTick: Long, tickDuration: Long)

class CandidateValueDStream(@transient ssc_ : StreamingContext,
    actionCollectorPort: Int,
    actionCollectorRecvBufferLength: Int,
    actionCollectorSendBufferLength: Int, 
    zooKeeperAddress: String,
    zooKeeperAppRoot: String,
    resolver: ComponentResolver[_ <: ResizingBuffer]) 
		extends NetworkInputDStream[CandidateValue](ssc_) with Logging {
  
	initLogging
  
	private var lastTickTime = 0l
	implicit val timeout = Timeout(30 seconds)
	val env = SparkEnv.get
	
	def getReceiver() = {
	  logInfo("Creating receiver")
	  val libraryPath = System.getProperty("java.library.path")
	  new ActionReceiver(actionCollectorPort, actionCollectorRecvBufferLength, 
	    actionCollectorSendBufferLength, MEMORY_ONLY, zooKeeperAddress, zooKeeperAppRoot, {
		   val kryo = Util.newKryoInstance();
		   Util.toKryoBytes(kryo, resolver)
	  	}, libraryPath)
	}
	
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
			val future = tickManager ? Tick(tickTime)
			try {
				Await.result(future, timeout.duration)
			} catch {
			  case (eTimedOut: TimeoutException) => {
			    logWarning("Timed out waiting for tick " + tickTime + 
			        ": proceeding without tick.")
			  }
			}
			lastTickTime = tickTime
		}
		super.compute(validTime)
    }

	private class TickManagerActor(zeroTime: Long, tickDuration: Duration) extends Actor {
		
		private var receiverActor: Option[ActorRef] = None
		private var lastTick: Long = zeroTime
		private val tickToCallerRefLookup = new Long2ObjectOpenHashMap[ActorRef]
		
		private val addressFunc = (ref: ActorRef) => ref.path.elements.reduce((s1, s2) => s1 + "/" + s2)
		
		override def preStart = {
		   logInfo("TickManagerActor registering at: " + addressFunc(self.actorRef))
		}
		
		override def postStop = {
		   tickToCallerRefLookup.values().foreach(callerRef => callerRef ! true)
		}
		
		def receive = {
			case RegisterTickReceiver(streamId: Int, receiverActor: ActorRef, objId: String) => {
			    logInfo("RegisterTickReceiver! StreamID=" + streamId + ", receiverActor=" + receiverActor + ", sender=" + sender)
				this.receiverActor = Some(receiverActor)
				sender ! Initialize(lastTick, tickDuration.milliseconds)
			}
			case Tick(time: Long) => {
				receiverActor match {
				  case Some(receiver) => {
				    tickToCallerRefLookup.put(time, sender)
				    receiver ! Tick(time)
				  }
				  case None => sender ! true
				}
				lastTick = time
			}
			case TickAck(time: Long) => {
				if (tickToCallerRefLookup.containsKey(time)) {
				   tickToCallerRefLookup.get(time) ! true
				}
			}
		}
	}

}

class ActionReceiver(
    actionCollectorPort: Int,
    actionCollectorRecvBufferLength: Int,
    actionCollectorSendBufferLength: Int,
    storageLevel: StorageLevel,  
	zooKeeperAddress: String,
	zooKeeperAppRoot: String,
	componentResolverBytes: Array[Byte],
	libraryPath: String) 
		extends NetworkReceiver[CandidateValue] with TickDelegate {

	private[this] case class TickProcessingDone(time: Long)
	
	/*
	 * We create a new actor system here rather than using the one provided
	 * by the spark environment on the worker executor so that we can use our 
	 * own message classes: the provided actor system uses a class loader 
	 * that doesn't include the dependencies of the driver class (and this
	 * receiver).
	 */
	lazy private val actorSystem = {
	  // get the host name of this worker
	  val host = {
	      val provider = env.actorSystem.asInstanceOf[ActorSystemImpl].provider
	      provider.asInstanceOf[RemoteActorRefProvider].transport.address.host.get
	  }
	  
	  val port = 0
	  
	  /*
	   * Taken from spark.util.AkkaUtils.createActorSystem which is unfortunately
	   * package private. Reusing spark settings.
	   */
	  val akkaThreads = System.getProperty("spark.akka.threads", "4").toInt
      val akkaBatchSize = System.getProperty("spark.akka.batchSize", "15").toInt
      val akkaTimeout = System.getProperty("spark.akka.timeout", "60").toInt
      val akkaFrameSize = System.getProperty("spark.akka.frameSize", "10").toInt
      val lifecycleEvents = System.getProperty("spark.akka.logLifecycleEvents", "false").toBoolean
	  
	  val conf = ConfigFactory.parseString("""
	      akka.daemonic = on
	      akka.event-handlers = ["akka.event.slf4j.Slf4jEventHandler"]
	      akka.stdout-loglevel = "ERROR"
	      akka.actor.provider = "akka.remote.RemoteActorRefProvider"
	      akka.remote.transport = "akka.remote.netty.NettyRemoteTransport"
	      akka.remote.netty.hostname = "%s"
	      akka.remote.netty.port = %d
	      akka.remote.netty.connection-timeout = %ds
	      akka.remote.netty.message-frame-size = %d MiB
	      akka.remote.netty.execution-pool-size = %d
	      akka.actor.default-dispatcher.throughput = %d
	      akka.remote.log-remote-lifecycle-events = %s
      """.format(host, port, akkaTimeout, akkaFrameSize, akkaThreads, akkaBatchSize,
                 if (lifecycleEvents) "on" else "off"))
	  
	  ActorSystem("ActionReceiver", conf, getClass.getClassLoader)
	}
	
	lazy private val tickActor = actorSystem.actorOf(
	    	Props(new TickReceiverActor(zooKeeperAddress, zooKeeperAppRoot)), "ActionCollector-" + streamId)
	    	
	protected def onStart() {
	    actorSystem
		tickActor
	}
	
	protected def onStop() {
		actorSystem.stop(tickActor)
		actorSystem.shutdown()
		actorSystem.awaitTermination()
	}
	
	def onTick(time: Long, candidateValuesIterator: java.util.Iterator[CandidateValue]) = {
		val candidateValues = new ArrayBuffer[CandidateValue]
		candidateValuesIterator.copyToBuffer(candidateValues)
		
		pushBlock("block-" + streamId + "-" + time, candidateValues, null, storageLevel)
		tickActor ! TickProcessingDone(time)
	}
	
	private class TickReceiverActor(
			zooKeeperAddress: String,
			zooKeeperAppRoot: String) extends Actor {
		val ip = System.getProperty("spark.driver.host", "localhost")
		val port = System.getProperty("spark.driver.port", "7077").toInt
		val url = "akka://spark@%s:%s/user/TickManagerActor-%d".format(ip, port, streamId)
		logInfo("TickManagerActorUrl: " + url)
		val tickManager = actorSystem.actorFor(url)
		implicit val timeout = Timeout(5 minutes)
		
		private val serviceContainerFactory = ServiceContainerFactory.newFactory(libraryPath)
	
		private var tickDuration = 0l
		private var lastTick = 0l
		private var actionCollector: Option[TickDriven] = None
		private var actionCollectorContainer: Option[ServiceContainer] = None

		override def preStart() {
		    self ! DoInit
		}
		
		override def postStop() {
			actionCollectorContainer match {
			  case Some(container) => container.close
			  case None =>
			}
		}
		
		override def receive() = {
			case DoInit => {
				logInfo("Registering with tick manager " + tickManager)
			  	tickManager ! RegisterTickReceiver(streamId, self, this.toString)
			    context.system.scheduler.scheduleOnce(5 minutes) {
					self ! DoInit
				}
			}
			case Initialize(lastTick: Long, tickDuration: Long) => initialize(lastTick, tickDuration)
			case ActionCollectorReady(actionCollector: TickDriven) => {
				this.actionCollector = Some(actionCollector)
				context.become(connected, true)
			}
			case Tick(time: Long) => {
			  logInfo("Received tick " + time + ", but the action collector is not yet ready. Returning ACK")
			  sender ! TickAck(time)
			}
			case TickProcessingDone(time: Long) =>  tickManager ! TickAck(time)
		}
		
		private def connected: Receive = {
		  	case Tick(time: Long) => {
			    actionCollector match {
			       case Some(collector) => {
			         if (!collector.tick(time)) {
			           // collector is not ready to process ticks yet,
			           // so just ACK
			           sender ! TickAck(time)
			         }
			       }
			       case None => sender ! TickAck(time)
			    }
			    lastTick = time
			}
			case TickProcessingDone(time: Long) => tickManager ! TickAck(time)
			case _ =>
		}
		
		private def initialize(lastTick: Long, tickDuration: Long) = {
			logInfo("Initializing")
			
			actionCollectorContainer match {
			  case Some(container) => {
			    logInfo("Closing existing container")
			    container.close()
			  }
			  case None =>
			}
			
			this.lastTick = lastTick
			this.tickDuration = tickDuration
			val container = serviceContainerFactory.create(
			    lastTick, 
			    tickDuration, 
			    ActionReceiver.this, 
			    actionCollector => self ! ActionCollectorReady(actionCollector),
			    e => stopOnError(e), 
			    stop, 
			    actionCollectorPort, 
			    actionCollectorRecvBufferLength, 
			    actionCollectorSendBufferLength, 
			    zooKeeperAddress, 
			    zooKeeperAppRoot, 
			    componentResolverBytes)
			actionCollectorContainer = Some(container)
			container.start()
			logInfo("Started Action Collector Container")
		}
		
	}
}

/*
 * Unfortunately an incompatible version of ZooKeeper is loaded by a parent
 * class loader of the one provided for this receiver. To overcome this,
 * we load the service container using a class loader that delegates to our
 * dependency first. The class loader of this receiver is known to be a URL
 * class loader that contains the paths to the driver dependencies on this
 * worker - we iterate through these to find the ZooKeeper jar.
 */
private object ServiceContainerFactory extends Logging {
  
  initLogging
  
  def newFactory(libraryPath: String): ServiceContainerFactory = {
        // ensure we have all required libraries on the path
		addLibraryPath(libraryPath)
    
		val providedClassLoader = getClass.getClassLoader.asInstanceOf[URLClassLoader]
		val passThroughList = (
		        "com.adamroughton.concentus.service.spark.ServiceContainerFactory" ::
		        "com.adamroughton.concentus.cluster.worker.ServiceContainer" ::
				"com.adamroughton.concentus.actioncollector.TickDelegate" ::
				"com.adamroughton.concentus.actioncollector.TickDriven" ::
				"com.adamroughton.concentus.data.ResizingBuffer" ::
				"com.adamroughton.concentus.data.model.kryo.CandidateValue" :: 
				"com.adamroughton.concentus.data.model.kryo.CandidateValueStrategy" ::
				"com.adamroughton.concentus.data.model.kryo.CandidateValueGroupKey" :: Nil).toArray
		
		/*
		 * Don't capture the scala-library as we want to share 
		 * scala objects between the current class loader and 
		 * the new one
		 */
		val urls = (for {
		  url <- providedClassLoader.getURLs
		  if (!url.getPath.contains("scala-library"))
		} yield url).toArray
				
		val containerClassLoader = new ParentLastURLClassLoader(urls, passThroughList, providedClassLoader)
		val containerFactoryClass = Class.forName("com.adamroughton.concentus.service.spark.ServiceContainerEntry", true, containerClassLoader)
		logInfo("Container class loader = " + containerFactoryClass.getClassLoader)
		containerFactoryClass.newInstance.asInstanceOf[ServiceContainerFactory]
  }
  
  private def addLibraryPath(path: String) {
     val usrPathsField = classOf[ClassLoader].getDeclaredField("usr_paths")
     usrPathsField.setAccessible(true)
     
     val usrPaths = usrPathsField.get(null).asInstanceOf[Array[String]]
     if (!usrPaths.contains(path)) {
        val newUsrPaths = new Array[String](usrPaths.length + 1)
        usrPaths.copyToArray(newUsrPaths)
        newUsrPaths(usrPaths.length) = path
        usrPathsField.set(null, newUsrPaths)
     }
  }
  
}

private trait ServiceContainerFactory {
   def create(
        startTime: Long,
        tickDuration: Long,
        tickDelegate: TickDelegate,
        actionCollectorReadyDelegate: (TickDriven) => Unit,
        stopOnErrorDelegate: (Exception) => Unit,
        stopDelegate: () => Unit,
		actionCollectorPort: Int,
	    actionCollectorRecvBufferLength: Int,
	    actionCollectorSendBufferLength: Int,
		zooKeeperAddress: String,
		zooKeeperAppRoot: String,
		componentResolverBytes: Array[Byte]): ServiceContainer
}

private class ServiceContainerEntry extends ServiceContainerFactory with Logging {
	
	initLogging
  
	def create(
	    startTime: Long,
        tickDuration: Long,
        tickDelegate: TickDelegate,
        actionCollectorReadyDelegate: (TickDriven) => Unit,
        stopOnErrorDelegate: (Exception) => Unit,
        stopDelegate: () => Unit,
	    actionCollectorPort: Int,
	    actionCollectorRecvBufferLength: Int,
	    actionCollectorSendBufferLength: Int,
		zooKeeperAddress: String,
		zooKeeperAppRoot: String,
		componentResolverBytes: Array[Byte]): ServiceContainer = {
			val clock = new DefaultClock
			
			// get the receiver's address
			val receiverAddress = {
			   val hostAddress = InetAddress.getLocalHost
			   if (hostAddress.isLoopbackAddress) {
			      val extAddrIterator = for { 
			        ni <- NetworkInterface.getNetworkInterfaces;
			        addr <- ni.getInetAddresses 
			    	   if addr.isInstanceOf[Inet4Address] && 
			    	       !addr.isLoopbackAddress && 
			    	       !addr.isLinkLocalAddress
			      } yield addr
			      if (!extAddrIterator.isEmpty) {
			         extAddrIterator.next
			      } else {
			         hostAddress
			      }
			   } else {
				   hostAddress
			   }
			}
			logInfo("Using receiver address " + receiverAddress)
			
			val actionCollectorDeployment = new ActionCollectorServiceDeployment(actionCollectorPort, -1, 
			    actionCollectorRecvBufferLength, actionCollectorSendBufferLength, tickDelegate, startTime, tickDuration) {
			   
			  override def createService[TBuffer <: ResizingBuffer](serviceId: Int, initData: StateData, context: 
			      ServiceContext[ServiceState], handle: ConcentusHandle, metricContext: MetricContext,
			      resolver: ComponentResolver[TBuffer]): ClusterService[ServiceState] = {
					  val service = super.createService(serviceId, initData, context, handle, metricContext, resolver)
							  .asInstanceOf[ActionCollectorService[_ <: ResizingBuffer]]
					  actionCollectorReadyDelegate(service)
			    	  service
			  }
			  
			}			
			
			/*
			 * Pass a custom Concentus handle that redirects termination calls/
			 * exceptions to Spark
			 */
			val concentusHandle = new ConcentusHandle(
			    clock, 
			    receiverAddress, 
			    zooKeeperAddress, 
			    zooKeeperAppRoot,
			    Collections.emptySet()) {
			  
			  override def signalFatalException(ex: Throwable) = {
			    stopOnErrorDelegate(new RuntimeException(ex))
			  }
			  
			  override def shutdown() = {
			    stopDelegate()
			  }
			  
			}
			
			val kryo = Util.newKryoInstance
			val componentResolver = Util.fromKryoBytes(kryo, componentResolverBytes, classOf[ComponentResolver[_ <: ResizingBuffer]])
			
			val clusterHandleSettings = new ClusterHandleSettings(zooKeeperAddress, 
			    zooKeeperAppRoot, concentusHandle)
			new ServiceContainerImpl(clusterHandleSettings, concentusHandle, 
			    actionCollectorDeployment, componentResolver)
	}
	
}