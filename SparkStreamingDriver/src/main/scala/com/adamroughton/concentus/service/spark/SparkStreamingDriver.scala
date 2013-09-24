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

class SparkStreamingDriver[TBuffer <: ResizingBuffer](
        receiverCount: Int,
		actionCollectorPort: Int,
		actionCollectorRecvBufferLength: Int,
		actionCollectorSendBufferLength: Int,
        sendQueueSize: Int,
		concentusHandle: ConcentusHandle, 
		metricContext: MetricContext,
		resolver: ComponentResolver[TBuffer]) 
			extends ConcentusServiceBase {
  
  	System.setProperty("spark.serializer", "spark.KryoSerializer")
    System.setProperty("spark.kryo.registrator", "concentus.service.spark.KryoRegistrator")
  
    val socketManager = resolver.newSocketManager(concentusHandle.getClock())
    var application: CollectiveApplication = null
    var masterUrl: String = null
    var canonicalStateProcessor: CanonicalStateProcessor[TBuffer] = null
    
    val sendQueue = {
       val sendHeader = new OutgoingEventHeader(0, 2)
       new SendQueue(
           "updateQueue",
           sendHeader,
           socketManager.newMessageQueueFactory(resolver.getEventQueueFactory())
    	.createSingleProducerQueue("updateSendQueue", sendQueueSize, 
    	    Constants.DEFAULT_MSG_BUFFER_SIZE, new YieldingWaitStrategy))
    }
	
	protected override def onInit(stateData: StateData, cluster: ClusterHandle) = {
    	application = cluster.getApplicationInstanceFactory().newInstance()
    	
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
 
    	val tickDuration = application.getTickDuration
    	val sparkMasterUrl = "spark://" + masterEndpoint.ipAddress + ":" + masterEndpoint.port
    	
    	val ssc = new StreamingContext(sparkMasterUrl, "Concentus", Milliseconds(tickDuration))
    	
    	// broadcast varId => topNCount mapping
    	val topNMap = ssc.sparkContext.broadcast(
    	    application.variableDefinitions map { v => (v.getVariableId, v.getTopNCount) } toMap)
    	
		val streams = for (id <- 1 to receiverCount) yield {
			val clusterHandleSettings = new ClusterHandleSettings(
			    cluster.settings.zooKeeperAddress,
			    cluster.settings.zooKeeperAppRoot,
			    concentusHandle)
			val stream = new CandidateValueDStream(ssc, actionCollectorPort, actionCollectorRecvBufferLength, 
			    actionCollectorSendBufferLength, id, clusterHandleSettings, resolver)
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
//			.foreach((rdd, time) => {
//			  val collectiveVarMap = new Int2ObjectOpenHashMap[CollectiveVariable](rdd.count.asInstanceOf[Int])
//			  rdd.foreach((varId, collectiveVar) => collectiveVarMap.put(varId, collectiveVar))
//			  canonicalStateProcessor.onTickCompleted(time.milliseconds, collectiveVarMap)
//			})
		ssc.start
	}

}

class KryoRegistrator extends SparkKyroRegistrator {
  
	override def registerClasses(kryo: Kryo) {
	   Util.initialiseKryo(kryo)
	}
  
}

object SparkStreamingDriver {
  val serviceInfo = new ServiceInfo("sparkStreamingDriver", classOf[ServiceState], SparkWorkerService.serviceInfo)
}

class SparkStreamingDriverDeployment(
		receiverCount: Int,
		actionCollectorPort: Int,
		actionCollectorRecvBufferLength: Int,
		actionCollectorSendBufferLength: Int,
		sendQueueSize: Int) extends ServiceDeploymentBase(SparkStreamingDriver.serviceInfo) {
  
  def this() = this(0, 0, 0, 0, 0)
  
  def onPreStart(stateData: StateData) = {}
  
  def createService[TBuffer <: ResizingBuffer](serviceId: Int,
      initData: StateData,
      serviceContext: ServiceContext[ServiceState],
      concentusHandle: ConcentusHandle,
      metricContext: MetricContext,
      resolver: ComponentResolver[TBuffer]): ClusterService[ServiceState] = {
    new SparkStreamingDriver(receiverCount, actionCollectorPort, actionCollectorRecvBufferLength, 
        actionCollectorSendBufferLength, sendQueueSize, concentusHandle, metricContext, resolver)
  }
}