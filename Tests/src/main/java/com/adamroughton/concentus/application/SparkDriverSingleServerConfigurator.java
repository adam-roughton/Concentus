package com.adamroughton.concentus.application;

import com.adamroughton.concentus.Constants;
import com.adamroughton.concentus.application.SparkDriverConfigurator.DataCache;
import com.adamroughton.concentus.crowdhammer.TestDeploymentSet;
import com.adamroughton.concentus.service.spark.SparkMasterServiceDeployment;
import com.adamroughton.concentus.service.spark.SparkSingleServerServiceDeployment;
import com.adamroughton.concentus.service.spark.SparkStreamingDriverDeployment;
import com.adamroughton.concentus.service.spark.SparkWorkerServiceDeployment;

class SparkDriverSingleServerConfigurator implements DeploymentConfigurator {
	
	@Override
	public TestDeploymentSet configure(TestDeploymentSet deploymentSet, int receiverCount) {
		return deploymentSet
				.addDeployment(new SparkSingleServerServiceDeployment(
						new SparkMasterServiceDeployment(DataCache.SPARK_HOME, 7077),
						new SparkWorkerServiceDeployment(DataCache.SPARK_HOME),
						new SparkStreamingDriverDeployment(DataCache.SPARK_HOME, DataCache.DRIVER_DEPENDENCIES, 1, -1, 
								Constants.MSG_BUFFER_ENTRY_LENGTH, Constants.MSG_BUFFER_ENTRY_LENGTH, -1, 
								Constants.MSG_BUFFER_ENTRY_LENGTH)), 1);
	}

	@Override
	public String deploymentName() {
		return "spark-single-server";
	}
	
}
