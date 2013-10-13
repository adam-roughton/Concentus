package com.adamroughton.concentus.application;

import java.util.concurrent.TimeUnit;

import com.adamroughton.concentus.application.CrowdAloud.Mode;
import com.adamroughton.concentus.crowdhammer.CrowdHammer;
import com.adamroughton.concentus.crowdhammer.ListClientCount;
import com.adamroughton.concentus.crowdhammer.Test;
import com.adamroughton.concentus.crowdhammer.TestDeploymentSet;
import com.adamroughton.concentus.clienthandler.ClientHandlerService.ClientHandlerServiceDeployment;

public class LocalTest {
	
	public static void main(String[] args) throws Exception {
		ApplicationVariant.SharedConfig.logUpdatesOneClientPerWorker = true;
		
		long[] tickDurations = new long[] { 100 };
		
		ListClientCount clientCountIterable = new ListClientCount(1000);
		//new SingleDisruptorConfigurator()
		DeploymentConfigurator[] depConfigs = new DeploymentConfigurator[] { new SparkDriverSingleServerConfigurator() };
		ApplicationVariant[] applicationVariants = new ApplicationVariant[] { new CollectivePong(1024), new Pixels() };
		Test test;
		for (DeploymentConfigurator deploymentConfigurator : depConfigs) {
			for (ApplicationVariant applicationVar : applicationVariants) {
				for (long tickDuration: tickDurations) {
					String testName = applicationVar.name() + "_tickRate=" + tickDuration;
						
					TestDeploymentSet testDeploymentSet = new TestDeploymentSet(deploymentConfigurator.deploymentName(), 
							applicationVar.getAgentFactory());
					deploymentConfigurator.configure(testDeploymentSet, 1)
						.addDeployment(new ClientHandlerServiceDeployment(-1, 2048, 2048), 1)
						.setWorkerCount(1);
					
					test = new Test(testName, applicationVar.getApplicationFactory(tickDuration), 
							testDeploymentSet, clientCountIterable, 1, TimeUnit.MINUTES);
					CrowdHammer.runTest(test);
				}
			}	
		}
	}
}
