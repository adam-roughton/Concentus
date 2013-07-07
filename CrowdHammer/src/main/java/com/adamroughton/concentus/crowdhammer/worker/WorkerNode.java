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

import com.adamroughton.concentus.ConcentusExecutableOperations;
import com.adamroughton.concentus.ConcentusHandle;
import com.adamroughton.concentus.ConcentusWorkerNode;
import com.adamroughton.concentus.crowdhammer.CrowdHammerServiceState;
import com.adamroughton.concentus.crowdhammer.config.CrowdHammerConfiguration;
import com.adamroughton.concentus.metric.MetricContext;

public class WorkerNode implements ConcentusWorkerNode<CrowdHammerConfiguration, CrowdHammerServiceState> {
	
	public static final String MAX_CLIENT_COUNT_OPTION = "n";
	
	public static void main(String[] args) {
		ConcentusExecutableOperations.executeClusterWorker(args, new WorkerNode());
	}
	
	@Override
	@SuppressWarnings("static-access")
	public Iterable<Option> getCommandLineOptions() {
		return Arrays.asList(OptionBuilder.withArgName("max client count")
				.hasArg()
				.isRequired(true)
				.withDescription("Maximum number of similated clients to run on this worker.")
				.create(MAX_CLIENT_COUNT_OPTION));
	}		

	@Override
	public String getProcessName() {
		return "CrowdHammer Worker";
	}

	@Override
	public Class<CrowdHammerConfiguration> getConfigType() {
		return CrowdHammerConfiguration.class;
	}
	
	@Override
	public WorkerService createService(Map<String, String> commandLineOptions,
			ConcentusHandle<? extends CrowdHammerConfiguration> handle,
			MetricContext metricContext) {
		int maxClientCount = Integer.parseInt(commandLineOptions.get(MAX_CLIENT_COUNT_OPTION));
		return new WorkerService(handle, maxClientCount, metricContext);
	}

	@Override
	public Class<CrowdHammerServiceState> getClusterStateClass() {
		return CrowdHammerServiceState.class;
	}

}
