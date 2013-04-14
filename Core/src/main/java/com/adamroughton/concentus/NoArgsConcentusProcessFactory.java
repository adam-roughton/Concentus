package com.adamroughton.concentus;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;

import org.apache.commons.cli.Option;

import com.adamroughton.concentus.ConcentusExecutableOperations.FactoryDelegate;
import com.adamroughton.concentus.config.Configuration;

public class NoArgsConcentusProcessFactory<TProcess, TConfig extends Configuration> implements ConcentusProcessFactory<TProcess, TConfig> {

	private final String _processName;
	private final FactoryDelegate<TProcess, TConfig> _factoryDelegate;
	private final Class<TProcess> _processType;
	private final Class<TConfig> _configType;
	
	public NoArgsConcentusProcessFactory(String processName, 
			FactoryDelegate<TProcess, TConfig> factoryDelegate, 
			Class<TProcess> processType, 
			Class<TConfig> configType) {
		_processName = Objects.requireNonNull(processName);
		_factoryDelegate = Objects.requireNonNull(factoryDelegate);
		_processType = Objects.requireNonNull(processType);
		_configType = Objects.requireNonNull(configType);
	}
	
	@Override
	public TProcess create(ConcentusHandle<? extends TConfig> concentusHandle,
			Map<String, String> commandLineOptions) {
		return _factoryDelegate.create(concentusHandle);
	}

	@Override
	public Iterable<Option> getCommandLineOptions() {
		return Collections.emptyList();
	}

	@Override
	public String getProcessName() {
		return _processName;
	}

	@Override
	public Class<TProcess> getSupportedType() {
		return _processType;
	}

	@Override
	public Class<TConfig> getConfigType() {
		return _configType;
	}

}