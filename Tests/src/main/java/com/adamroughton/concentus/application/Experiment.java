package com.adamroughton.concentus.application;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.javatuples.Pair;

import com.adamroughton.concentus.application.ExperimentUtil.ExperimentApplicationFactory;
import com.adamroughton.concentus.application.ExperimentUtil.ExperimentClientAgentFactory;
import com.adamroughton.concentus.application.ExperimentUtil.TestVariable;
import com.adamroughton.concentus.clienthandler.ClientHandlerService.ClientHandlerServiceDeployment;
import com.adamroughton.concentus.crowdhammer.CrowdHammer;
import com.adamroughton.concentus.crowdhammer.ListClientCount;
import com.adamroughton.concentus.crowdhammer.Test;
import com.adamroughton.concentus.crowdhammer.TestDeploymentSet;

import static com.adamroughton.concentus.application.ExperimentUtil.*;

public class Experiment {
	
	public static void main(String[] args) throws Exception {
		long[] tickDurations = new long[] { 1000, 500, 100 };
		int[] valDataRanges = new int[] { 16, 128, 1024 * 1024 };
		
		List<Pair<String, TestVariable[]>> variableConfigurations = new ArrayList<>();
		variableConfigurations.add(new Pair<>("SingleVar_Range32_Top1", new TestVariable[] { new TestVariable(1, 1000, 1, 32, 1) }));
		variableConfigurations.add(new Pair<>("SingleVar_Range100000_Top50", new TestVariable[] { new TestVariable(50, 1000, 4, 100000, 4) }));
		variableConfigurations.add(new Pair<>("5Vars_Range32_Top1", createTestVarSet(5, 1, 1000, 1, 32, 1)));
		variableConfigurations.add(new Pair<>("1_100000RangeVar_5Vars_Range32_Top1", 
				append(new TestVariable[] { new TestVariable(50, 1000, 4, 100000, 4) }, createTestVarSet(5, 1, 1000, 1, 32, 1))));
		variableConfigurations.add(new Pair<>((1024 * 1024) + "Vars_Range32_Top1", createTestVarSet(1024 * 1024, 1, 1000, 1, 32, 1)));
		
		ListClientCount clientCountIterable = new ListClientCount(1000, 5000, 10000, 15000, 20000, 25000, 30000, 50000, 100000);
		
//		ListClientCount clientCountIterable = new ListClientCount(1000, 2000, 3000, 4000, 5000, 6000, 7000, 8000, 
//				9000, 10000, 11000, 12000, 13000, 14000, 15000, 16000, 17000, 18000, 19000, 20000, 21000,
//				22000, 23000, 24000, 25000, 26000, 27000, 28000, 29000, 30000, 50000, 100000);
		DeploymentConfigurator[] depConfigs = new DeploymentConfigurator[] { new SingleDisruptorConfigurator() };
		
//		for (long tickDuration: tickDurations) {
//			for (int valDataRange : valDataRanges) {
//				for (Pair<String, TestVariable[]> variableConf : variableConfigurations) {
//					
//					ExperimentApplicationFactory applicationFactory = new ExperimentApplicationFactory(100, 1, var);
//					ExperimentClientAgentFactory agentFactory = new ExperimentClientAgentFactory(1, new int[] {8});
//					
//					
//				}
//				for (int collectiveVariableCount : collectiveVariableCounts) {
//				String testName = "probe";
//				
//				
//				
//				
//				TestDeploymentSet testDeploymentSet = new TestDeploymentSet("w" + workerCount + "_ch" + clientHandlerCount, agentFactory);
//				pretestConfigurator.configure(testDeploymentSet, 1)
//					.addDeployment(new ClientHandlerServiceDeployment(-1, 2048, 2048), clientHandlerCount)
//					.setWorkerCount(workerCount);
//				
//				test = new Test(testName, applicationFactory, testDeploymentSet, clientCountIterable, 2, TimeUnit.MINUTES);
//				CrowdHammer.runTest(test);
//				}
//			}
//		}	
	}
}
