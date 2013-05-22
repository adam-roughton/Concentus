/*
 * Copyright 2013 Adam Roughton
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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