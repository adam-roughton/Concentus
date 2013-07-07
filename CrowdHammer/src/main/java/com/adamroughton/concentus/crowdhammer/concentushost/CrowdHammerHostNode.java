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
package com.adamroughton.concentus.crowdhammer.concentushost;

import java.util.Arrays;
import java.util.Map;
import java.util.Objects;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;

import com.adamroughton.concentus.ConcentusExecutableOperations;
import com.adamroughton.concentus.ConcentusHandle;
import com.adamroughton.concentus.ConcentusServiceState;
import com.adamroughton.concentus.ConcentusWorkerNode;
import com.adamroughton.concentus.cluster.worker.ClusterListener;
import com.adamroughton.concentus.config.Configuration;
import com.adamroughton.concentus.crowdhammer.CrowdHammerServiceState;
import com.adamroughton.concentus.crowdhammer.config.CrowdHammerConfiguration;
import com.adamroughton.concentus.metric.MetricContext;
import com.adamroughton.concentus.util.Util;

public class CrowdHammerHostNode implements ConcentusWorkerNode<CrowdHammerConfiguration, CrowdHammerServiceState> {

	public static final String PROCESS_NAME = "CrowdHammer Worker";
	public static final String CONCENTUS_NODE_CLASS_OPTION = "c";
	
	private final ConcentusWorkerNode<Configuration, ConcentusServiceState> _adaptedNode;
	
	public CrowdHammerHostNode(ConcentusWorkerNode<Configuration, ConcentusServiceState> adaptedNode) {
		_adaptedNode = Objects.requireNonNull(adaptedNode);
	}
	
	public static void main(String[] args) {
		/*
		 * We first get the adapted node class passed as an argument
		 */
		Map<String, String> commandLineArgs = ConcentusExecutableOperations.parseCommandLine(PROCESS_NAME, Arrays.asList(getServiceOption()), args, true);
		String concentusNodeClassName = commandLineArgs.get(CONCENTUS_NODE_CLASS_OPTION);
		ConcentusWorkerNode<Configuration, ConcentusServiceState> adaptedNode = getAdaptedNode(concentusNodeClassName);
		
		/*
		 * Execute the adapted node
		 */
		ConcentusExecutableOperations.executeClusterWorker(args, new CrowdHammerHostNode(adaptedNode));
	}
	
	@SuppressWarnings("static-access")
	private static Option getServiceOption() {
		return OptionBuilder.withArgName("concentus node class")
				.hasArg()
				.isRequired(true)
				.withDescription("fully qualified class name of the concentus node to wrap.")
				.create(CONCENTUS_NODE_CLASS_OPTION);
	}
	
	@SuppressWarnings("unchecked")
	private static ConcentusWorkerNode<Configuration, ConcentusServiceState> getAdaptedNode(String concentusNodeClassName) {
		try {
			Class<?> concentusServiceFactoryClass = Class.forName(concentusNodeClassName);
			Object object = concentusServiceFactoryClass.newInstance();
			
			if (!(object instanceof ConcentusWorkerNode<?, ?>)) {
				throw new RuntimeException(String.format("The class %s is not of type %s", 
						concentusNodeClassName,
						ConcentusWorkerNode.class.getName()));
			}
			ConcentusWorkerNode<?,?> node = (ConcentusWorkerNode<?, ?>) object;
			
			if (!node.getConfigType().equals(Configuration.class)) {
				throw new RuntimeException(String.format("This host node only wraps worker nodes that use a configuration of type %1$s. " +
						"The given node class %2$s expected a %3$s typed configuration.", 
						Configuration.class.getName(), node.getClass().getName(), node.getConfigType().getName()));
			}
			
			if (!node.getClusterStateClass().equals(ConcentusServiceState.class)) {
				throw new RuntimeException(String.format("This host node only wraps worker nodes using a service state type of %1$s. " +
						"The given node class %2$s expected a %3$s service state.", 
						Configuration.class.getName(), node.getClass().getName(), node.getClusterStateClass().getName()));
			}
			return (ConcentusWorkerNode<Configuration, ConcentusServiceState>) node;
		} catch (ClassNotFoundException eNotFound){
			throw new RuntimeException(String.format("Could not find the worker node class '%1$s'.", concentusNodeClassName), eNotFound);
		} catch (InstantiationException | IllegalAccessException | SecurityException e) {
			throw new RuntimeException(String.format("Could not instantiate the worker node class %1$s.", concentusNodeClassName), e);
		}
	}

	@Override
	@SuppressWarnings("static-access")
	public Iterable<Option> getCommandLineOptions() {
		return Util.newIterable(_adaptedNode.getCommandLineOptions(),
				OptionBuilder.withArgName("service process factory class")
				.hasArgs()
				.isRequired(true)
				.withDescription("fully qualified class name of the service process factory class.")
				.create(CONCENTUS_NODE_CLASS_OPTION));
	}

	@Override
	public String getProcessName() {
		return PROCESS_NAME;
	}

	@Override
	public Class<CrowdHammerConfiguration> getConfigType() {
		return CrowdHammerConfiguration.class;
	}

	@Override
	public ClusterListener<CrowdHammerServiceState> createService(
			Map<String, String> commandLineOptions,
			ConcentusHandle<? extends CrowdHammerConfiguration> handle,
			MetricContext metricContext) {
		return new ConcentusServiceAdapter(handle, _adaptedNode, commandLineOptions, metricContext);
	}

	@Override
	public Class<CrowdHammerServiceState> getClusterStateClass() {
		return CrowdHammerServiceState.class;
	}
	
}
