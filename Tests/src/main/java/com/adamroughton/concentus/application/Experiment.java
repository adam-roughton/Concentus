package com.adamroughton.concentus.application;

import java.util.concurrent.TimeUnit;

import com.adamroughton.concentus.crowdhammer.CrowdHammer;
import com.adamroughton.concentus.crowdhammer.ListClientCount;
import com.adamroughton.concentus.crowdhammer.Test;
import com.adamroughton.concentus.crowdhammer.TestDeploymentSet;
import com.adamroughton.concentus.clienthandler.ClientHandlerService.ClientHandlerServiceDeployment;

public class Experiment {
	
	public static void main(String[] args) throws Exception {
		long[] tickDurations = new long[] { 1000, 500, 100 };
		
//		ListClientCount clientCountIterable = new ListClientCount(1000, 2000, 3000, 4000, 5000, 6000, 7000, 8000, 
//				9000, 10000, 11000, 12000, 13000, 14000, 15000, 16000, 17000, 18000, 19000, 20000, 21000,
//				22000, 23000, 24000, 25000, 26000, 27000, 28000, 29000, 30000, 50000, 100000);
		ListClientCount clientCountIterable = new ListClientCount(1000);
		DeploymentConfigurator[] depConfigs = new DeploymentConfigurator[] { new SingleDisruptorConfigurator() };
		ApplicationVariant[] applicationVariants = new ApplicationVariant[] { new CrowdAloud(), new CollectivePong(), new Pixels() };
		Test test;
		for (DeploymentConfigurator deploymentConfigurator : depConfigs) {
			for (ApplicationVariant applicationVar : applicationVariants) {
				for (long tickDuration: tickDurations) {
					String testName = applicationVar.name() + "_t" + tickDuration;
						
					TestDeploymentSet testDeploymentSet = new TestDeploymentSet(testName, applicationVar.getAgentFactory());
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
