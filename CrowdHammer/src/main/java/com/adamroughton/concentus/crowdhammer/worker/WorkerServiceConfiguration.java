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
package com.adamroughton.concentus.crowdhammer.worker;

import java.util.Arrays;
import java.util.Map;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;

import com.adamroughton.concentus.CommandLineConfiguration;

public class WorkerServiceConfiguration implements CommandLineConfiguration<WorkerService> {

	public static final String MAX_CLIENT_COUNT_OPTION = "n";
	
	@SuppressWarnings("static-access")
	@Override
	public Iterable<Option> getCommandLineOptions() {
		return Arrays.asList(OptionBuilder.withArgName("max client count")
				.hasArgs()
				.isRequired(true)
				.withDescription("Maximum number of similated clients to run on this worker.")
				.create(MAX_CLIENT_COUNT_OPTION));
	}

	@Override
	public void configure(
			WorkerService workerService,
			Map<String, String> cmdLineValues) {
		int maxClientCount = Integer.parseInt(cmdLineValues.get(MAX_CLIENT_COUNT_OPTION));
		workerService.setMaxClientCount(maxClientCount);
	}

}
