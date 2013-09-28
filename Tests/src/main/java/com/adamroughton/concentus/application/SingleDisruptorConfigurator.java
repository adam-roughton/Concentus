package com.adamroughton.concentus.application;

import com.adamroughton.concentus.Constants;
import com.adamroughton.concentus.canonicalstate.direct.DirectCanonicalStateService.DirectCanonicalStateServiceDeployment;
import com.adamroughton.concentus.crowdhammer.TestDeploymentSet;

class SingleDisruptorConfigurator implements DeploymentConfigurator {

	@Override
	public TestDeploymentSet configure(TestDeploymentSet deploymentSet,
			int receiverCount) {
		return deploymentSet.addDeployment(new DirectCanonicalStateServiceDeployment(-1, -1, Constants.MSG_BUFFER_ENTRY_LENGTH, 
				Constants.MSG_BUFFER_ENTRY_LENGTH, -1, Constants.MSG_BUFFER_ENTRY_LENGTH), 1);
	}

	@Override
	public String deploymentName() {
		return "singleDisruptor";
	}

}
