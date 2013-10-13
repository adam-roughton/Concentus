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
import com.adamroughton.concentus.cluster.worker.ServiceDeployment
import org.apache.commons.io.FileUtils
import java.nio.file.Files

class SparkMasterService(
    _config: ConcentusSparkConfig,
    masterAddress: String,
    masterPort: Int,
    serviceId: Int,
    serviceContext: ServiceContext[ServiceState],
    concentusHandle: ConcentusHandle) 
		extends ExternalProcessServiceBase("SparkMaster", serviceContext, concentusHandle) with ScratchSpaceUser {
  
  def config = _config
  
  override def onBind(stateData: StateData, cluster: ClusterHandle) = {
     val sparkRunCmd = Paths.get(config.sparkHome).resolve("run").toString
     startProcess(sparkRunCmd, "spark.deploy.master.Master", "-i", masterAddress, "-p", masterPort.toString);
     
     // wait 10 seconds for master process to start and bind
     Thread.sleep(10000)
     Log.info("Started spark master at spark://" + masterAddress + ":" + masterPort);
    
     val sparkMasterEndpoint = new ServiceEndpoint(serviceId, SparkMasterService.masterEndpointType, 
         masterAddress, masterPort)
	 cluster.registerServiceEndpoint(sparkMasterEndpoint)
  }

}

object SparkMasterService {
  
  val serviceInfo = new ServiceInfo("sparkMaster", classOf[ServiceState])
  val masterEndpointType = "sparkMaster"
  
}

class SparkMasterServiceDeployment(config: ConcentusSparkConfig, masterPort: Int) 
	extends ServiceDeploymentBase(SparkMasterService.serviceInfo) {
  
  def this() = this(null, 7077)
  
  def onPreStart(stateData: StateData) = {}
  
  def createService[TBuffer <: ResizingBuffer](serviceId: Int,
      initData: StateData,
      serviceContext: ServiceContext[ServiceState],
      concentusHandle: ConcentusHandle,
      metricContext: MetricContext,
      resolver: ComponentResolver[TBuffer]): ClusterService[ServiceState] = {
    val masterAddress = concentusHandle.getNetworkAddress.getHostAddress
    new SparkMasterService(config, masterAddress, masterPort, serviceId, serviceContext, concentusHandle)
  }
  
}