package com.adamroughton.concentus.service.spark

import com.adamroughton.concentus.cluster.worker.ClusterService
import com.adamroughton.concentus.data.cluster.kryo.ServiceState
import com.adamroughton.concentus.cluster.worker.StateData
import com.adamroughton.concentus.cluster.worker.ClusterHandle
import org.apache.commons.io.FileUtils
import java.nio.file.Paths
import java.nio.file.Files
import spark.Logging

private case class ArgList(list: List[String]) {
	    
	def ::(condAndArg: (Boolean, String)): ArgList = {
		if (condAndArg._1) {
	    	new ArgList(condAndArg._2 :: list)
	    } else {
	    	this      
	    }
	}
     
	def :::(condAndArg: (Boolean, List[String])): ArgList = {
		if (condAndArg._1) {
			new ArgList(condAndArg._2 ::: list)
		} else {
			this      
		}
	}
     
	def ::(item: String) : ArgList = {
		new ArgList(item :: list)
	}
     
	def toList(): List[String] = {
		list
	}
}

class ConcentusSparkConfig(var _sparkHome: String, var _scratchDir: String, var _isDistributed: Boolean, 
    var _memoryProperty: String = "2g", var _logLifecycleEvents: Boolean = false) {
  
  // for Kryo
  private[this] def this() = this(null, null, false, null, false)
  
  def sparkHome = _sparkHome
  def scratchDir = _scratchDir
  def isDistributed = _isDistributed
  def memoryProperty = _memoryProperty
  def logLifecycleEvents = _logLifecycleEvents
  
}


trait ScratchSpaceUser extends ClusterService[ServiceState] with Logging {
 
  def config: ConcentusSparkConfig
  val manageDir = this.isInstanceOf[SparkMasterService] || config.isDistributed
  val scratchPath = Paths.get(config.scratchDir)
  
  abstract protected override def onStateChanged(newServiceState: ServiceState, stateChangeIndex: Int, 
      stateData: StateData, cluster: ClusterHandle) = {
    newServiceState match {
      case ServiceState.INIT if manageDir => {
    	// create directory before initialising
        logInfo("Creating scratch dir " + scratchPath.toString)
	    FileUtils.deleteQuietly(scratchPath.toFile)
	    Files.createDirectory(scratchPath)
	    
	    super.onStateChanged(newServiceState, stateChangeIndex, stateData, cluster)
      }
      case ServiceState.SHUTDOWN if manageDir => {
        super.onStateChanged(newServiceState, stateChangeIndex, stateData, cluster)
        
        // remove directory after shutting down service
        logInfo("Removing scratch dir " + scratchPath.toString)
        FileUtils.deleteQuietly(scratchPath.toFile)
      }
      case _ => super.onStateChanged(newServiceState, stateChangeIndex, stateData, cluster)
    }
  }
  
}