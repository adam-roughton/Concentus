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

class SparkMasterService(
    masterAddress: String,
    masterPort: Int,
    sparkHome: Path,
    serviceContext: ServiceContext[ServiceState]) 
		extends ExternalProcessServiceBase(serviceContext) {
  
  override def onBind(stateData: StateData, cluster: ClusterHandle) = {
     val sparkMasterCommand = sparkHome.resolve("run").toString() + 
    	" spark.deploy.master.Master -i " + masterAddress + " -p " + masterPort
     startProcess(sparkMasterCommand);
     Log.info("Started spark master at spark://" + masterAddress + ":" + masterPort);
    
     val sparkMasterEndpoint = new ServiceEndpoint(SparkMasterService.masterEndpointType, 
         masterAddress, masterPort)
	 cluster.registerServiceEndpoint(sparkMasterEndpoint)
  }
  
}

object SparkMasterService {
  
  val serviceInfo = new ServiceInfo("sparkMaster", classOf[ServiceState])
  val masterEndpointType = "sparkMaster"
  
}

class SparkMasterServiceDeployment(masterPort: Int, sparkHome: Path) extends ServiceDeploymentBase(SparkMasterService.serviceInfo) {
  
  def this() = this(7077, null)
  
  def onPreStart(stateData: StateData) = {}
  
  def createService[TBuffer <: ResizingBuffer](serviceId: Int,
      initData: StateData,
      serviceContext: ServiceContext[ServiceState],
      concentusHandle: ConcentusHandle,
      metricContext: MetricContext,
      resolver: ComponentResolver[TBuffer]): ClusterService[ServiceState] = {
    val masterAddress = concentusHandle.getNetworkAddress.getHostAddress
    new SparkMasterService(masterAddress, masterPort, sparkHome, serviceContext)
  }
  
}