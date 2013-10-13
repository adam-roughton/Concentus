package com.adamroughton.concentus.service.spark

import com.adamroughton.concentus.ExternalProcessServiceBase
import com.adamroughton.concentus.cluster.worker.ServiceContext
import com.adamroughton.concentus.data.cluster.kryo.ServiceState
import com.adamroughton.concentus.ConcentusHandle
import com.adamroughton.concentus.cluster.worker.StateData
import com.adamroughton.concentus.cluster.worker.ClusterHandle
import com.adamroughton.concentus.cluster.worker.ServiceDeploymentBase
import com.adamroughton.concentus.data.cluster.kryo.ServiceInfo
import com.adamroughton.concentus.data.ResizingBuffer
import com.adamroughton.concentus.cluster.worker.ClusterService
import com.adamroughton.concentus.ComponentResolver
import com.adamroughton.concentus.metric.MetricContext
import com.adamroughton.concentus.data.cluster.kryo.ServiceEndpoint
import java.nio.file.Path
import com.esotericsoftware.minlog.Log
import java.nio.file.Paths
import scala.collection.mutable.ListBuffer
import scala.collection.JavaConversions._
import com.adamroughton.concentus.actioncollector.ActionCollectorService

class SparkWorkerService(sparkHome: String, workerPort: Int, workerWebUIPort: Int, serviceContext: ServiceContext[ServiceState], concentusHandle: ConcentusHandle) 
		extends ExternalProcessServiceBase("SparkWorker", serviceContext, concentusHandle) {
  
  override def onBind(stateData: StateData, cluster: ClusterHandle) = {
     val masterEndpoints = cluster.getAllServiceEndpoints(SparkMasterService.masterEndpointType)
     val masterEndpoint = if (masterEndpoints.size < 1) {
       throw new RuntimeException("There are no spark master services registered! Cannot start spark worker.");
     } else {
       masterEndpoints.get(0)
     }     
     val sparkRunCmd = Paths.get(sparkHome).resolve("run").toString
     Log.info("Starting spark worker")
     
     val workerArgs = "spark.deploy.worker.Worker" :: 
    	 (workerPort != 0, "-p" :: workerPort.toString :: Nil) ::: 
       	 (workerWebUIPort != 0, "--webui-port" :: workerWebUIPort.toString :: Nil) ::: 
    	 ("spark://" + masterEndpoint.ipAddress + ":" + masterEndpoint.port) :: 
    	 ArgList(Nil)
       
     startProcess(sparkRunCmd, workerArgs.toList)
     
     // wait 10 seconds for the worker process to start
     Thread.sleep(10000)
     
     Log.info("Started spark worker");
  } 
  
}

object SparkWorkerService {
  
  val serviceInfo = new ServiceInfo("sparkWorker", classOf[ServiceState], SparkMasterService.serviceInfo)
  
}

class SparkWorkerServiceDeployment(sparkHome: String, workerPort: Int, workerWebUIPort: Int) 
	extends ServiceDeploymentBase[ServiceState](SparkWorkerService.serviceInfo, ActionCollectorService.SERVICE_INFO) {
    
  def this(sparkHome: String) = this(sparkHome, 0, 0)
  
  def this() = this(null, 0, 0)
  
  def onPreStart(stateData: StateData) = {}
  
  def createService[TBuffer <: ResizingBuffer](serviceId: Int,
      initData: StateData,
      serviceContext: ServiceContext[ServiceState],
      concentusHandle: ConcentusHandle,
      metricContext: MetricContext,
      resolver: ComponentResolver[TBuffer]): ClusterService[ServiceState] = {
    new SparkWorkerService(sparkHome, workerPort, workerWebUIPort, serviceContext, concentusHandle)
  }
  
}