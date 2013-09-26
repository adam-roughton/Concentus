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

class SparkWorkerService(sparkHome: String, workerPort: Int, workerWebUIPort: Int, serviceContext: ServiceContext[ServiceState], concentusHandle: ConcentusHandle) 
		extends ExternalProcessServiceBase(serviceContext, concentusHandle) {
  
  override def onBind(stateData: StateData, cluster: ClusterHandle) = {
     val masterEndpoints = cluster.getAllServiceEndpoints(SparkMasterService.masterEndpointType)
     val masterEndpoint = if (masterEndpoints.size < 1) {
       throw new RuntimeException("There are no spark master services registered! Cannot start spark worker.");
     } else {
       masterEndpoints.get(0)
     }     
     val sparkWorkerCommand = Paths.get(sparkHome).resolve("run").toString
     Log.info("Starting spark worker")
     startProcess(sparkWorkerCommand, "spark.deploy.worker.Worker", "-p", workerPort.toString, 
         "--webui-port", workerWebUIPort.toString, "spark://" + masterEndpoint.ipAddress + ":" + masterEndpoint.port)
     Log.info("Started spark worker");
  }
  
}

object SparkWorkerService {
  
  val serviceInfo = new ServiceInfo("sparkWorker", classOf[ServiceState], SparkMasterService.serviceInfo)
  
}

class SparkWorkerServiceDeployment(sparkHome: String, workerPort: Int, workerWebUIPort: Int) 
	extends ServiceDeploymentBase[ServiceState](SparkWorkerService.serviceInfo) {
    
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