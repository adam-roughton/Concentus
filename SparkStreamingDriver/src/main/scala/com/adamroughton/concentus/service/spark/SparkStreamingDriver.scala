package com.adamroughton.concentus.service.spark

import spark.streaming.StreamingContext
import spark.streaming.Milliseconds
import spark.RDD
import spark.{KryoRegistrator => SparkKyroRegistrator}
import spark.streaming.StreamingContext._
import com.adamroughton.concentus.data.ResizingBuffer
import com.adamroughton.concentus.util.Util
import com.adamroughton.concentus.model.CollectiveApplication
import com.adamroughton.concentus.data.model.kryo.CollectiveVariable
import com.esotericsoftware.minlog.Log
import com.esotericsoftware.kryo.Kryo
import com.adamroughton.concentus.canonicalstate.CanonicalStateProcessor
import com.adamroughton.concentus.config.ConfigurationUtil
import com.adamroughton.concentus.canonicalstate.TickTimer.TickStrategy
import com.lmax.disruptor.YieldingWaitStrategy
import com.adamroughton.concentus.messaging.OutgoingEventHeader
import com.adamroughton.concentus.messaging.patterns.SendQueue
import com.adamroughton.concentus.metric.{MetricContext, MetricGroup}
import com.adamroughton.concentus.{ConcentusHandle, Constants, ComponentResolver}
import com.adamroughton.concentus.cluster.ClusterHandleSettings
import com.adamroughton.concentus.data.cluster.kryo.{ServiceState, ServiceInfo}
import com.adamroughton.concentus.cluster.worker.{ConcentusServiceBase, StateData, ClusterHandle, 
  ServiceContext, ClusterService, ServiceDeploymentBase}
import it.unimi.dsi.fastutil.ints.{Int2ObjectArrayMap, Int2ObjectOpenHashMap}
import java.util.concurrent.Executors
import com.adamroughton.concentus.pipeline.ProcessingPipeline
import com.adamroughton.concentus.pipeline.PipelineProcess
import com.adamroughton.concentus.messaging.zmq.SocketSettings
import org.zeromq.ZMQ
import com.adamroughton.concentus.messaging.MessagingUtil
import com.adamroughton.concentus.messaging.Publisher
import java.util.concurrent.TimeUnit
import com.adamroughton.concentus.CoreServices
import com.adamroughton.concentus.data.cluster.kryo.ServiceEndpoint
import com.adamroughton.concentus.actioncollector.ActionCollectorService
import java.net.URLDecoder
import scala.collection.JavaConversions

class SparkStreamingDriver[TBuffer <: ResizingBuffer](
        sparkHome: String,
        jarFilePaths: Array[String],
        receiverCount: Int,
		actionCollectorPort: Int,
		actionCollectorRecvBufferLength: Int,
		actionCollectorSendBufferLength: Int,
		canonicalStateUpdatePort: Int,
        sendQueueSize: Int,
        serviceId: Int,
		concentusHandle: ConcentusHandle, 
		metricContext: MetricContext,
		resolver: ComponentResolver[TBuffer]) 
			extends ConcentusServiceBase {
  
  	System.setProperty("spark.serializer", "spark.KryoSerializer")
    System.setProperty("spark.kryo.registrator", "com.adamroughton.concentus.service.spark.KryoRegistrator")
  
    val executor = Executors.newCachedThreadPool()
    val socketManager = resolver.newSocketManager(concentusHandle.getClock())
    var application: CollectiveApplication = null
    var masterUrl: String = null
    var canonicalStateProcessor: CanonicalStateProcessor[TBuffer] = null
    val pubHeader = new OutgoingEventHeader(0, 2)
  	val pubEventQueue = socketManager.newMessageQueueFactory(resolver.getEventQueueFactory())
    	.createSingleProducerQueue("updateSendQueue", sendQueueSize, 
    	    Constants.DEFAULT_MSG_BUFFER_SIZE, new YieldingWaitStrategy)
    var pipeline: ProcessingPipeline[TBuffer] = null
	
	protected override def onInit(stateData: StateData, cluster: ClusterHandle) = {
    	application = cluster.getApplicationInstanceFactory().newInstance()
    	
    	val sendQueue = new SendQueue(
           "updateQueue",
           pubHeader,
           pubEventQueue)
    	
    	// create the canonical state processor
    	canonicalStateProcessor = new CanonicalStateProcessor(application, 
    	    sendQueue,
    		new MetricGroup,
    		metricContext)
	}
	
	protected override def onBind(stateData: StateData, cluster: ClusterHandle) = {  
    	val masterEndpoints = cluster.getAllServiceEndpoints(SparkMasterService.masterEndpointType)
        val masterEndpoint = if (masterEndpoints.size < 1) {
          throw new RuntimeException("There are no spark master services registered! Cannot start spark driver.");
        } else {
          masterEndpoints.get(0)
        }
    	
    	val zooKeeperAddress = cluster.settings.zooKeeperAddress
		val zooKeeperAppRoot = cluster.settings.zooKeeperAppRoot
 
    	val tickDuration = application.getTickDuration
    	val sparkMasterUrl = "spark://" + masterEndpoint.ipAddress + ":" + masterEndpoint.port
    	
    	Log.info("Creating spark context")
    	val ssc = new StreamingContext(sparkMasterUrl, "Concentus", Milliseconds(tickDuration), sparkHome, jarFilePaths, Map())
    	
    	// broadcast varId => topNCount mapping
    	val topNMap = ssc.sparkContext.broadcast(
    	    application.variableDefinitions map { v => (v.getVariableId, v.getTopNCount) } toMap)
    	
		val streams = for (id <- 1 to receiverCount) yield {
			val stream = new CandidateValueDStream(ssc, actionCollectorPort, actionCollectorRecvBufferLength, 
			    actionCollectorSendBufferLength, id, zooKeeperAddress, zooKeeperAppRoot, resolver)
			ssc.registerInputStream(stream)

			stream.map(v => (v.groupKey, v))
				.reduceByKey((v1, v2) => v1.union(v2))
		}
		val combinedStream = streams.reduce((s1, s2) => s1.union(s2))
			.reduceByKey((v1, v2) => v1.union(v2))
			.map { 
				case (k, v) => {			  
					(v.getVariableId, new CollectiveVariable(topNMap.value(v.getVariableId), v))
				} 
			}
			.reduceByKey((v1, v2) => v1.union(v2))
			.foreach((rdd, time) => {
			  val collectiveVarMap = new Int2ObjectOpenHashMap[CollectiveVariable](rdd.count.asInstanceOf[Int])
			  rdd.foreach(idVarPair => collectiveVarMap.put(idVarPair._1, idVarPair._2))
			  canonicalStateProcessor.onTickCompleted(time.milliseconds, collectiveVarMap)
			})
		Log.info("Starting spark context")
		ssc.start
		Log.info("Spark context started")
		
		// set up canonical state pub socket
		
		val pubSocketSettings = SocketSettings.create()
			.bindToPort(canonicalStateUpdatePort);
		val pubSocketId = socketManager.create(ZMQ.PUB, pubSocketSettings, "pubSocket")
		val pubSocketMessenger = socketManager.getSocketMutex(pubSocketId)
		val statePublisher = MessagingUtil.asSocketOwner("canonicalStatePublisher", pubEventQueue, new Publisher(pubHeader), pubSocketMessenger)
		
		// have an empty process up front in place of spark
		pipeline = ProcessingPipeline.build[TBuffer](new Runnable() { 
		  def run() = {
		    try {
		      val waitMonitor = new Object
		      waitMonitor.synchronized {
		        waitMonitor.wait()
		      }
		    } catch {
		      case eInterrupted: InterruptedException => {
		         ssc.stop
		      }
		    }
		  } 
		}, concentusHandle.getClock())
		.thenConnector(pubEventQueue)
		.then(statePublisher)
		.createPipeline(executor)
		
		// register the publish endpoint
		val address = concentusHandle.getNetworkAddress.getHostAddress
		val pubEndpoint = new ServiceEndpoint(serviceId, CoreServices.CANONICAL_STATE.getId, address, canonicalStateUpdatePort)
		cluster.registerServiceEndpoint(pubEndpoint)
	}
	
	override def onStart(stateData: StateData, cluster: ClusterHandle) = {
	  pipeline.start
	}
	
	override def onShutdown(stateData: StateData, cluster: ClusterHandle) = {
	  if (pipeline != null) {
		  pipeline.halt(30, TimeUnit.SECONDS)
	  }
	}
	
}

class KryoRegistrator extends SparkKyroRegistrator {
  
	override def registerClasses(kryo: Kryo) {
	   Util.initialiseKryo(kryo)
	}
  
}

object SparkStreamingDriver {
  val serviceInfo = new ServiceInfo(CoreServices.CANONICAL_STATE.getId(), classOf[ServiceState], SparkWorkerService.serviceInfo)
}

class SparkStreamingDriverDeployment(
		sparkHome: String,
		jarFilePaths: Array[String],
		receiverCount: Int,
		actionCollectorPort: Int,
		actionCollectorRecvBufferLength: Int,
		actionCollectorSendBufferLength: Int,
		canonicalStateUpdatePort: Int,
		sendQueueSize: Int) extends ServiceDeploymentBase[ServiceState](
		    SparkStreamingDriver.serviceInfo, 
		    ActionCollectorService.SERVICE_INFO) {
  
  def this() = this(null, Array[String](), 0, 0, 0, 0, 0, 0)
		    
  def onPreStart(stateData: StateData) = {}
  
  def createService[TBuffer <: ResizingBuffer](serviceId: Int,
      initData: StateData,
      serviceContext: ServiceContext[ServiceState],
      concentusHandle: ConcentusHandle,
      metricContext: MetricContext,
      resolver: ComponentResolver[TBuffer]): ClusterService[ServiceState] = {
    new SparkStreamingDriver(sparkHome, jarFilePaths, receiverCount, actionCollectorPort, actionCollectorRecvBufferLength, 
        actionCollectorSendBufferLength, canonicalStateUpdatePort, sendQueueSize, serviceId, concentusHandle, metricContext, resolver)
  }
}