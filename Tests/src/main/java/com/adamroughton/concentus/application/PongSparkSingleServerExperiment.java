package com.adamroughton.concentus.application;

import java.util.concurrent.TimeUnit;

import com.adamroughton.concentus.clienthandler.ClientHandlerService.ClientHandlerServiceDeployment;
import com.adamroughton.concentus.crowdhammer.CrowdHammer;
import com.adamroughton.concentus.crowdhammer.ListClientCount;
import com.adamroughton.concentus.crowdhammer.Test;
import com.adamroughton.concentus.crowdhammer.TestDeploymentSet;

public class PongSparkSingleServerExperiment {

	public static void main(String[] args) throws Exception {
		ApplicationVariant.SharedConfig.logUpdatesOneClientPerWorker = false;
		
		long[] tickDurations = new long[] { 500, 100 };
		
		ListClientCount clientCountIterable = new ListClientCount(5000, 10000, 15000, 20000, 25000, 30000, 35000, 40000, 45000, 50000);
		//new SingleDisruptorConfigurator()
		//new SparkDriverConfigurator() 
		DeploymentConfigurator[] depConfigs = new DeploymentConfigurator[] { new SparkDriverSingleServerConfigurator("4g") };
		ApplicationVariant[] applicationVariants = new ApplicationVariant[] { new CollectivePong(1024) };
		Test test;
		for (DeploymentConfigurator deploymentConfigurator : depConfigs) {
			for (ApplicationVariant applicationVar : applicationVariants) {
				for (long tickDuration: tickDurations) {
					String testName = applicationVar.name() + "_tickRate=" + tickDuration;
						
					TestDeploymentSet testDeploymentSet = new TestDeploymentSet(deploymentConfigurator.deploymentName(), 
							applicationVar.getAgentFactory());
					deploymentConfigurator.configure(testDeploymentSet, 1)
						.addDeployment(new ClientHandlerServiceDeployment(-1, 2048, 2048), 4)
						.setWorkerCount(4);
					
					test = new Test(testName, applicationVar.getApplicationFactory(tickDuration), 
							testDeploymentSet, clientCountIterable, 1, TimeUnit.MINUTES);
					CrowdHammer.runTest(test);
				}
			}	
		}
	}
	
}
