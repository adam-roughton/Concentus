package com.adamroughton.concentus.service.spark

import spark.streaming.StreamingContext
import spark.streaming.Milliseconds
import spark.RDD
import spark.{KryoRegistrator => SparkKyroRegistrator}
import spark.streaming.StreamingContext._
import com.adamroughton.concentus.data.ResizingBuffer
import com.adamroughton.concentus.ConcentusHandle
import com.adamroughton.concentus.config.Configuration
import com.adamroughton.concentus.metric.MetricContext
import com.adamroughton.concentus.util.Util
import com.adamroughton.concentus.model.CollectiveApplication
import com.adamroughton.concentus.data.model.kryo.CollectiveVariable
import com.esotericsoftware.minlog.Log
import com.esotericsoftware.kryo.Kryo
import com.adamroughton.concentus.canonicalstate.CanonicalStateProcessor
import com.adamroughton.concentus.config.ConfigurationUtil
import com.adamroughton.concentus.Constants
import com.adamroughton.concentus.canonicalstate.TickTimer.TickStrategy
import com.lmax.disruptor.YieldingWaitStrategy
import com.adamroughton.concentus.messaging.OutgoingEventHeader
import com.adamroughton.concentus.messaging.patterns.SendQueue
import com.adamroughton.concentus.metric.MetricGroup
import com.adamroughton.concentus.cluster.worker.ConcentusServiceBase
import com.adamroughton.concentus.ComponentResolver
import com.adamroughton.concentus.cluster.worker.StateData
import com.adamroughton.concentus.cluster.worker.ClusterHandle
import com.adamroughton.concentus.data.cluster.kryo.ServiceState
import com.adamroughton.concentus.cluster.ClusterHandleSettings

class SparkStreamingDriver[TBuffer <: ResizingBuffer](
		actionCollectorPort: Int,
		actionCollectorRecvBufferLength: Int,
		actionCollectorSendBufferLength: Int,
        sendQueueSize: Int,
		concentusHandle: ConcentusHandle, 
		metricContext: MetricContext,
		resolver: ComponentResolver[TBuffer]) 
			extends ConcentusServiceBase {
  
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
    	/*
    	 * We need spark to start the action collectors on the workers
    	 * before moving to the next state as dependent services will look
    	 * for them after bind.
    	 */
    	System.setProperty("spark.serializer", "spark.KryoSerializer")
    	System.setProperty("spark.kryo.registrator", "concentus.service.spark.KryoRegistrator")
	  
    	val tickDuration = application.getTickDuration
    	val sparkMasterUrl = ""
    	val sparkWorkerCount = 10
    	
    	val ssc = new StreamingContext(sparkMasterUrl, "Concentus", Milliseconds(tickDuration))
    	
    	// broadcast varId => topNCount mapping
    	val topNMap = ssc.sparkContext.broadcast(
    	    application.variableDefinitions map { v => (v.getVariableId, v.getTopNCount) } toMap)
    	
		val streams = for (id <- 1 to sparkWorkerCount) yield {
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
			.foreach(rdd => rdd.collect())
		ssc.start
	}

}

class KryoRegistrator extends SparkKyroRegistrator {
  
	override def registerClasses(kryo: Kryo) {
	   Util.initialiseKryo(kryo)
	}
  
}