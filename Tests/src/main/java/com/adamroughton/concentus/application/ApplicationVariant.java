package com.adamroughton.concentus.application;

import com.adamroughton.concentus.InstanceFactory;
import com.adamroughton.concentus.crowdhammer.ClientAgent;
import com.adamroughton.concentus.model.CollectiveApplication;

public interface ApplicationVariant {

	InstanceFactory<? extends CollectiveApplication> getApplicationFactory(long tickDuration);
	
	InstanceFactory<? extends ClientAgent> getAgentFactory();
	
	String name();
}
