package com.adamroughton.concentus.application;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.javatuples.Pair;

import com.adamroughton.concentus.clienthandler.ClientHandlerService.ClientHandlerServiceDeployment;
import com.adamroughton.concentus.crowdhammer.CrowdHammer;
import com.adamroughton.concentus.crowdhammer.ListClientCount;
import com.adamroughton.concentus.crowdhammer.Test;
import com.adamroughton.concentus.crowdhammer.TestDeploymentSet;

import static com.adamroughton.concentus.application.ExperimentUtil.*;

public class Pretest {

	public static void main(String[] args) throws Exception {
		ListClientCount clientCountIterable = new ListClientCount(2000, 4000, 6000, 8000, 
				10000, 12000, 14000, 16000, 18000, 20000);
		DeploymentConfigurator pretestConfigurator = new SingleDisruptorConfigurator();

		List<Pair<Integer, Integer>> workerClientHandlerDeploymentPairs = new ArrayList<>();
		int availableNodes = 8;
		for (int i = 1; i < availableNodes; i++) {
			workerClientHandlerDeploymentPairs.add(new Pair<>(i, availableNodes - i));
		}
		
		Test test;
		for (Pair<Integer, Integer> workerClientHandlerPair : workerClientHandlerDeploymentPairs) {
			int workerCount = workerClientHandlerPair.getValue0();
			int clientHandlerCount = workerClientHandlerPair.getValue1();
			
			String testName = "pretest_w" + workerCount + "_ch" + clientHandlerCount;
			TestVariable var = new TestVariable(25, 1000, 8, 10000, 8);
			ExperimentApplicationFactory applicationFactory = new ExperimentApplicationFactory(100, 1, var);
			ExperimentClientAgentFactory agentFactory = new ExperimentClientAgentFactory(1, new int[] {8});
			
			TestDeploymentSet testDeploymentSet = new TestDeploymentSet("w" + workerCount + "_ch" + clientHandlerCount, agentFactory);
			pretestConfigurator.configure(testDeploymentSet, 1)
				.addDeployment(new ClientHandlerServiceDeployment(-1, 2048, 2048), clientHandlerCount)
				.setWorkerCount(workerCount);
			
			test = new Test(testName, applicationFactory, testDeploymentSet, clientCountIterable, 2, TimeUnit.MINUTES);
			CrowdHammer.runTest(test);
		}
	}
}
