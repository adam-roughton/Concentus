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
package com.adamroughton.consentus.crowdhammer;

import java.util.Arrays;
import java.util.Map;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;

import com.adamroughton.consentus.CommandLineConfiguration;

public class CrowdHammerServiceConfiguration implements CommandLineConfiguration<CrowdHammerService> {

	public static final char SERVICE_CLASS_OPTION = 's';
	
	@SuppressWarnings("static-access")
	@Override
	public Iterable<Option> getCommandLineOptions() {
		return Arrays.asList(
				OptionBuilder.withArgName("service class")
				.hasArgs()
				.isRequired(true)
				.withDescription("fully qualified class name of the service class.")
				.create(SERVICE_CLASS_OPTION));
	}

	@Override
	public void configure(CrowdHammerService process,
			Map<String, String> cmdLineValues) {
		// TODO Auto-generated method stub
		
	}

}
