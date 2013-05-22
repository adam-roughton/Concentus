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
