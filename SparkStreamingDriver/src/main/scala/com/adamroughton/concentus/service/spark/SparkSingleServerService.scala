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
import com.adamroughton.concentus.cluster.worker.ConcentusServiceBase
import com.adamroughton.concentus.cluster.ClusterHandleSettings
import com.adamroughton.concentus.cluster.worker.ServiceContainerImpl
import com.adamroughton.concentus.actioncollector.ActionCollectorService

class SparkSingleServerService(masterDeployment: SparkMasterServiceDeployment, 
    workerDeployment: SparkWorkerServiceDeployment, driverDeployment: SparkStreamingDriverDeployment,
    concentusHandle: ConcentusHandle, resolver: ComponentResolver[_ <: ResizingBuffer]) extends ConcentusServiceBase {
  
  val zooKeeperAddress = concentusHandle.getZooKeeperAddress
  val zooKeeperAppRoot = concentusHandle.getZooKeeperAppRoot
  
  val clusterHandleSettings = new ClusterHandleSettings(zooKeeperAddress, 
			    zooKeeperAppRoot, concentusHandle)
  
  val sparkMasterServiceContainer = new ServiceContainerImpl(clusterHandleSettings, 
      concentusHandle, 
      masterDeployment, 
      resolver)
  
  val sparkWorkerServiceContainer = new ServiceContainerImpl(clusterHandleSettings, 
      concentusHandle, 
      workerDeployment, 
      resolver)
  
  val sparkDriverServiceContainer = new ServiceContainerImpl(clusterHandleSettings, 
      concentusHandle, 
      driverDeployment, 
      resolver)
  
  override def onStart(stateData: StateData, cluster: ClusterHandle) = {
	 sparkMasterServiceContainer.start
	 sparkWorkerServiceContainer.start
	 sparkDriverServiceContainer.start
  } 
  
  override def onShutdown(stateData: StateData, cluster: ClusterHandle) = {
	 sparkMasterServiceContainer.close
	 sparkWorkerServiceContainer.close
	 sparkDriverServiceContainer.close
  } 
  
}

object SparkSingleServerService {
  
  val serviceInfo = new ServiceInfo("sparkSingleServiceService", classOf[ServiceState])
  
}

class SparkSingleServerServiceDeployment(masterDeployment: SparkMasterServiceDeployment, 
    workerDeployment: SparkWorkerServiceDeployment, driverDeployment: SparkStreamingDriverDeployment) 
	extends ServiceDeploymentBase[ServiceState](SparkSingleServerService.serviceInfo, 
	    SparkMasterService.serviceInfo, 
	    SparkWorkerService.serviceInfo, 
	    SparkStreamingDriver.serviceInfo,
	    ActionCollectorService.SERVICE_INFO) {
  
  def this() = this(null, null, null)
  
  def onPreStart(stateData: StateData) = {}
  
  def createService[TBuffer <: ResizingBuffer](serviceId: Int,
      initData: StateData,
      serviceContext: ServiceContext[ServiceState],
      concentusHandle: ConcentusHandle,
      metricContext: MetricContext,
      resolver: ComponentResolver[TBuffer]): ClusterService[ServiceState] = {
    new SparkSingleServerService(masterDeployment, workerDeployment, 
        driverDeployment, concentusHandle, resolver)
  }
  
}