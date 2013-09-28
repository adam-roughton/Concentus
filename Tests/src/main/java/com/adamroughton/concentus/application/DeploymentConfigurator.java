package com.adamroughton.concentus.application;

import com.adamroughton.concentus.crowdhammer.TestDeploymentSet;

interface DeploymentConfigurator {

	TestDeploymentSet configure(TestDeploymentSet deploymentSet, int maxReceiverCount);
	
	String deploymentName();
}
