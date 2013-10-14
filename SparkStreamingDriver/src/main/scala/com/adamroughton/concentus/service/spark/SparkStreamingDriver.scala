package com.adamroughton.concentus.service.spark

import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import scala.Array.canBuildFrom
import org.zeromq.ZMQ
import com.adamroughton.concentus.ComponentResolver
import com.adamroughton.concentus.ConcentusHandle
import com.adamroughton.concentus.Constants
import com.adamroughton.concentus.CoreServices
import com.adamroughton.concentus.canonicalstate.CanonicalStateProcessor
import com.adamroughton.concentus.cluster.worker.ClusterHandle
import com.adamroughton.concentus.cluster.worker.ClusterService
import com.adamroughton.concentus.cluster.worker.ConcentusServiceBase
import com.adamroughton.concentus.cluster.worker.ServiceContext
import com.adamroughton.concentus.cluster.worker.ServiceDeploymentBase
import com.adamroughton.concentus.cluster.worker.StateData
import com.adamroughton.concentus.data.ResizingBuffer
import com.adamroughton.concentus.data.cluster.kryo.ServiceEndpoint
import com.adamroughton.concentus.data.cluster.kryo.ServiceInfo
import com.adamroughton.concentus.data.cluster.kryo.ServiceState
import com.adamroughton.concentus.data.model.kryo.CollectiveVariable
import com.adamroughton.concentus.messaging.MessagingUtil
import com.adamroughton.concentus.messaging.OutgoingEventHeader
import com.adamroughton.concentus.messaging.Publisher
import com.adamroughton.concentus.messaging.patterns.SendQueue
import com.adamroughton.concentus.messaging.zmq.SocketSettings
import com.adamroughton.concentus.metric.MetricContext
import com.adamroughton.concentus.metric.MetricGroup
import com.adamroughton.concentus.model.CollectiveApplication
import com.adamroughton.concentus.pipeline.ProcessingPipeline
import com.adamroughton.concentus.util.Util
import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.minlog.Log
import com.lmax.disruptor.YieldingWaitStrategy
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap
import spark.{KryoRegistrator => SparkKyroRegistrator}
import spark.streaming.Milliseconds
import spark.streaming.StreamingContext
import spark.streaming.StreamingContext.toPairDStreamFunctions
import com.adamroughton.concentus.ConcentusEndpoints
import scala.collection.JavaConversions._
import spark.SparkEnv
import akka.actor.Actor

class SparkStreamingDriver[TBuffer <: ResizingBuffer](
        _config: ConcentusSparkConfig,
        jarFilePaths: Array[String],
        receiverCount: Int,
		actionCollectorPort: Int,
		actionCollectorRecvBufferLength: Int,
		actionCollectorSendBufferLength: Int,
		requestedCanonicalStateUpdatePort: Int,
        sendQueueSize: Int,
        serviceId: Int,
		concentusHandle: ConcentusHandle, 
		metricContext: MetricContext,
		resolver: ComponentResolver[TBuffer]) 
			extends ConcentusServiceBase with ScratchSpaceUser {
  
	def config = _config
  
    System.setProperty("spark.executor.memory", config.memoryProperty)
	System.setProperty("spark.serializer", "spark.KryoSerializer")
    System.setProperty("spark.kryo.registrator", "com.adamroughton.concentus.service.spark.KryoRegistrator")
    System.setProperty("spark.akka.logLifecycleEvents", config.logLifecycleEvents.toString)
    System.setProperty("spark.local.dir", config.scratchDir)
  
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
    	val ssc = new StreamingContext(sparkMasterUrl, "Concentus", Milliseconds(tickDuration), config.sparkHome, jarFilePaths, Map())
    	
    	val variableDefinitions = application.variableDefinitions
    	val collectiveVarCount = variableDefinitions.length
    	val collectiveVariablePartitioner = new CollectiveVariablePartitioner(collectiveVarCount)
    	
    	// broadcast varId => topNCount mapping
    	val topNMap = ssc.sparkContext.broadcast(
    	    variableDefinitions map { v => (v.getVariableId, v.getTopNCount) } toMap)
    	
    	Log.info("Starting streams with receiver count " + receiverCount)
		val streams = for (i <- 1 to receiverCount) yield {
			val stream = new CandidateValueDStream(ssc, actionCollectorPort, actionCollectorRecvBufferLength, 
			    actionCollectorSendBufferLength, zooKeeperAddress, zooKeeperAppRoot, resolver)
			ssc.registerInputStream(stream)
			
			// perform a reduce for the candidate values on each receiver first
			stream.mapPartitions(it => it.map(v => (v.groupKey, v)), true)
				.reduceByKey((v1, v2) => v1.union(v2))
		}
		streams.reduce((s1, s2) => s1.union(s2))
			/*
			 * Do the final reduce for the candidate values across all receivers.
			 * We use the default HashPartitioner for this on the CandidateValue
			 * group key.
			 */ 
			.reduceByKey((v1, v2) => v1.union(v2))
			.map { 
				case (k, v) => (v.getVariableId, new CollectiveVariable(topNMap.value(v.getVariableId), v))
			}
			.reduceByKey((v1: CollectiveVariable, v2: CollectiveVariable) => v1.union(v2), collectiveVariablePartitioner)
			.foreach((rdd, time) => {
				val idVarPairs = rdd.collect
				val collectiveVarMap = new Int2ObjectOpenHashMap[CollectiveVariable](idVarPairs.length)				
				for (idVarPair <- idVarPairs) {
				  collectiveVarMap.put(idVarPair._1, idVarPair._2)
				}
				canonicalStateProcessor.onTickCompleted(time.milliseconds, collectiveVarMap)
			})
		Log.info("Starting spark context")
		ssc.start
		Log.info("Spark context started")
		
		// set up canonical state pub socket
		
		val pubSocketSettings = SocketSettings.create()
			.bindToPort(requestedCanonicalStateUpdatePort);
		val pubSocketId = socketManager.create(ZMQ.PUB, pubSocketSettings, "pubSocket")
		val canonicalStateUpdatePort = socketManager.getBoundPort(pubSocketId)
		
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
		         Log.info("Stopping Spark Streaming Context...")
		         ssc.stop
		         Log.info("Spark Streaming Context Stopped")
		      }
		    }
		  } 
		}, concentusHandle.getClock())
		.thenConnector(pubEventQueue)
		.then(statePublisher)
		.createPipeline(executor)
		
		// register the publish endpoint
		val address = concentusHandle.getNetworkAddress.getHostAddress
		val pubEndpoint = new ServiceEndpoint(serviceId, ConcentusEndpoints.CANONICAL_STATE_PUB.getId(), address, canonicalStateUpdatePort)
		cluster.registerServiceEndpoint(pubEndpoint)
	}
	
	protected override def onStart(stateData: StateData, cluster: ClusterHandle) = {
	  pipeline.start
	}
	
	protected override def onShutdown(stateData: StateData, cluster: ClusterHandle) = {
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

/**
 * Partition collective variables by variable ID
 */
class CollectiveVariablePartitioner(collectiveVarCount: Int) extends spark.Partitioner {
  
  override def numPartitions: Int = collectiveVarCount
  
  override def getPartition(key: Any): Int = key.asInstanceOf[Int]

}

object SparkStreamingDriver {
  val serviceInfo = new ServiceInfo(CoreServices.CANONICAL_STATE.getId(), classOf[ServiceState], SparkWorkerService.serviceInfo)
}

class SparkStreamingDriverDeployment(
		config: ConcentusSparkConfig,
		jarFilePaths: Array[String],
		receiverCount: Int,
		actionCollectorPort: Int,
		actionCollectorRecvBufferLength: Int,
		actionCollectorSendBufferLength: Int,
		canonicalStateUpdatePort: Int,
		sendQueueSize: Int) extends ServiceDeploymentBase[ServiceState](
		    SparkStreamingDriver.serviceInfo) {
  
  def this() = this(null, Array[String](), 0, 0, 0, 0, 0, 0)
		    
  def onPreStart(stateData: StateData) = {}
  
  def createService[TBuffer <: ResizingBuffer](serviceId: Int,
      initData: StateData,
      serviceContext: ServiceContext[ServiceState],
      concentusHandle: ConcentusHandle,
      metricContext: MetricContext,
      resolver: ComponentResolver[TBuffer]): ClusterService[ServiceState] = {
    new SparkStreamingDriver(config, jarFilePaths, receiverCount, actionCollectorPort, actionCollectorRecvBufferLength, 
        actionCollectorSendBufferLength, canonicalStateUpdatePort, sendQueueSize, serviceId, concentusHandle, metricContext, resolver)
  }
}