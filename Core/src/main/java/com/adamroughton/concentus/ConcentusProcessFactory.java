package com.adamroughton.concentus;

import java.util.Map;

import org.apache.commons.cli.Option;

import com.adamroughton.concentus.config.Configuration;

public interface ConcentusProcessFactory<TProcess, TConfig extends Configuration> {

	TProcess create(ConcentusHandle<? extends TConfig> concentusHandle, Map<String, String> commandLineOptions);
	
	Iterable<Option> getCommandLineOptions();
	
	String getProcessName();
	
	Class<TProcess> getSupportedType();
	
	Class<TConfig> getConfigType();
}
