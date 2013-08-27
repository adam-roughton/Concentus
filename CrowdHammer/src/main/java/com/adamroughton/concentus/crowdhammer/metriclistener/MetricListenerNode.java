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
package com.adamroughton.concentus.crowdhammer.metriclistener;

import java.util.Collections;
import java.util.Map;

import org.apache.commons.cli.Option;

import com.adamroughton.concentus.ConcentusExecutableOperations;
import com.adamroughton.concentus.ConcentusHandle;
import com.adamroughton.concentus.ConcentusWorkerNode;
import com.adamroughton.concentus.cluster.worker.ClusterListener;
import com.adamroughton.concentus.crowdhammer.CrowdHammerServiceState;
import com.adamroughton.concentus.crowdhammer.config.CrowdHammerConfiguration;
import com.adamroughton.concentus.data.ResizingBuffer;
import com.adamroughton.concentus.metric.MetricContext;

public class MetricListenerNode implements ConcentusWorkerNode<CrowdHammerConfiguration, CrowdHammerServiceState> {

	public static void main(String[] args) {
		ConcentusExecutableOperations.executeClusterWorker(args, new MetricListenerNode());
	}

	@Override
	public Iterable<Option> getCommandLineOptions() {
		return Collections.emptyList();
	}

	@Override
	public String getProcessName() {
		return "Metric Listener";
	}

	@Override
	public Class<CrowdHammerConfiguration> getConfigType() {
		return CrowdHammerConfiguration.class;
	}

	@Override
	public <TBuffer extends ResizingBuffer> ClusterListener<CrowdHammerServiceState> createService(
			Map<String, String> commandLineOptions,
			ConcentusHandle<? extends CrowdHammerConfiguration, TBuffer> handle,
			MetricContext metricContext) {
		return new MetricListenerService<>(handle);
	}

	@Override
	public Class<CrowdHammerServiceState> getClusterStateClass() {
		return CrowdHammerServiceState.class;
	}

}
