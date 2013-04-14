package com.adamroughton.concentus.crowdhammer;

import java.util.Map;

import org.apache.commons.cli.Option;

import com.adamroughton.concentus.ConcentusHandle;
import com.adamroughton.concentus.crowdhammer.config.CrowdHammerConfiguration;

public interface CrowdHammerServiceFactory {

	CrowdHammerService create(ConcentusHandle<CrowdHammerConfiguration> concentusHandle, Map<String, String> commandLineOptions);
	
	Iterable<Option> getCommandLineOptions();
	
}
