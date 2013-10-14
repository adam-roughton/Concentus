package com.adamroughton.concentus.application;

import java.util.Objects;

import com.adamroughton.concentus.Constants;
import com.adamroughton.concentus.application.SparkDriverConfigurator.DataCache;
import com.adamroughton.concentus.crowdhammer.TestDeploymentSet;
import com.adamroughton.concentus.service.spark.ConcentusSparkConfig;
import com.adamroughton.concentus.service.spark.SparkMasterServiceDeployment;
import com.adamroughton.concentus.service.spark.SparkSingleServerServiceDeployment;
import com.adamroughton.concentus.service.spark.SparkStreamingDriverDeployment;
import com.adamroughton.concentus.service.spark.SparkWorkerServiceDeployment;

class SparkDriverSingleServerConfigurator implements DeploymentConfigurator {
	
	private final String _memoryProperty;
	
	public SparkDriverSingleServerConfigurator(String memoryProperty) {
		_memoryProperty = Objects.requireNonNull(memoryProperty);
	}
	
	@Override
	public TestDeploymentSet configure(TestDeploymentSet deploymentSet, int receiverCount) {
		ConcentusSparkConfig config = new ConcentusSparkConfig(DataCache.SPARK_HOME, DataCache.SPARK_SCRATCH, false, _memoryProperty, false);
		return deploymentSet
				.addDeployment(new SparkSingleServerServiceDeployment(
						new SparkMasterServiceDeployment(config, 7077),
						new SparkWorkerServiceDeployment(config),
						new SparkStreamingDriverDeployment(config, DataCache.DRIVER_DEPENDENCIES, 1, -1, 
								Constants.MSG_BUFFER_ENTRY_LENGTH, Constants.MSG_BUFFER_ENTRY_LENGTH, -1, 
								Constants.MSG_BUFFER_ENTRY_LENGTH)), 1);
	}

	@Override
	public String deploymentName() {
		return "spark-single-server";
	}
	
}
