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
import com.adamroughton.concentus.ConcentusNode;
import com.adamroughton.concentus.ConcentusProcessFactory;
import com.adamroughton.concentus.ConcentusService;
import com.adamroughton.concentus.config.Configuration;
import com.adamroughton.concentus.crowdhammer.CrowdHammerService;
import com.adamroughton.concentus.crowdhammer.config.CrowdHammerConfiguration;
import com.adamroughton.concentus.util.Util;

public class CrowdHammerHostNode implements ConcentusNode<CrowdHammerService, CrowdHammerConfiguration> {

	public static final String PROCESS_NAME = "CrowdHammer Worker";
	public static final String CONCENTUS_NODE_CLASS_OPTION = "c";
	
	private final ConcentusProcessFactory<ConcentusService, Configuration> _serviceFactory;
	
	public CrowdHammerHostNode(ConcentusProcessFactory<ConcentusService, Configuration> serviceFactory) {
		_serviceFactory = Objects.requireNonNull(serviceFactory);
	}
	
	@Override
	public ConcentusProcessFactory<CrowdHammerService, CrowdHammerConfiguration> getProcessFactory() {
		return new AdapterFactory(_serviceFactory);
	}
	
	public static void main(String[] args) {
		/*
		 * We first get the service factory class passed as an argument
		 */
		Map<String, String> commandLineArgs = ConcentusExecutableOperations.parseCommandLine(PROCESS_NAME, Arrays.asList(getServiceOption()), args, true);
		String concentusNodeClassName = commandLineArgs.get(CONCENTUS_NODE_CLASS_OPTION);
		ConcentusNode<?, ?> concentusNode = getConcentusNode(concentusNodeClassName);
		
		/*
		 * Execute the wrapped service 
		 */
		ConcentusExecutableOperations.executeClusterWorker(args, new CrowdHammerHostNode(getServiceFactory(concentusNode)));
	}
	
	@SuppressWarnings("static-access")
	private static Option getServiceOption() {
		return OptionBuilder.withArgName("concentus node class")
				.hasArgs()
				.isRequired(true)
				.withDescription("fully qualified class name of the concentus node to wrap.")
				.create(CONCENTUS_NODE_CLASS_OPTION);
	}
	
	private static ConcentusNode<?, ?> getConcentusNode(String concentusNodeClassName) {
		try {
			Class<?> concentusServiceFactoryClass = Class.forName(concentusNodeClassName);
			Object object = concentusServiceFactoryClass.newInstance();
			
			if (!(object instanceof ConcentusNode<?, ?>)) {
				throw new RuntimeException(String.format("The class %s is not a concentus node", 
						concentusNodeClassName));
			}
			return (ConcentusNode<?, ?>) object;
		} catch (ClassNotFoundException eNotFound){
			throw new RuntimeException(String.format("Could not find the service factory class '%1$s'.", concentusNodeClassName), eNotFound);
		} catch (InstantiationException | IllegalAccessException | SecurityException e) {
			throw new RuntimeException(String.format("Could not instantiate the service factory class %1$s.", concentusNodeClassName), e);
		}
	}
	
	@SuppressWarnings("unchecked")
	private static ConcentusProcessFactory<ConcentusService, Configuration> getServiceFactory(ConcentusNode<?, ?> concentusNode) {
		ConcentusProcessFactory<?, ?> processFactory = concentusNode.getProcessFactory();
		
		if (!ConcentusService.class.isAssignableFrom(processFactory.getSupportedType())) {
			throw new RuntimeException(String.format("This host node only supports processes of type %1$s. The given node class %2$s does not create %1$s typed processes.", 
					ConcentusService.class.getName(), concentusNode.getClass().getName()));
		}
		if (!processFactory.getConfigType().equals(Configuration.class)) {
			throw new RuntimeException(String.format("This host node only supports processes that uses a configuration of type %1$s. " +
					"The given node class %2$s expected a %3$s typed configuration.", 
					Configuration.class.getName(), concentusNode.getClass().getName(), processFactory.getConfigType().getName()));
		}
		return (ConcentusProcessFactory<ConcentusService, Configuration>) processFactory;
	}

	private static final class AdapterFactory implements ConcentusProcessFactory<CrowdHammerService, CrowdHammerConfiguration> {

		private final ConcentusProcessFactory<ConcentusService, Configuration> _serviceFactory;
		
		public AdapterFactory(ConcentusProcessFactory<ConcentusService, Configuration> serviceFactory) {
			_serviceFactory = serviceFactory;
		}
		
		@Override
		public CrowdHammerService create(
				ConcentusHandle<? extends CrowdHammerConfiguration> concentusHandle,
				Map<String, String> commandLineOptions) {
			return new ConcentusServiceAdapter(concentusHandle, _serviceFactory, commandLineOptions);
		}

		@Override
		@SuppressWarnings("static-access")
		public Iterable<Option> getCommandLineOptions() {
			return Util.newIterable(_serviceFactory.getCommandLineOptions(),
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
		public Class<CrowdHammerService> getSupportedType() {
			return CrowdHammerService.class;
		}

		@Override
		public Class<CrowdHammerConfiguration> getConfigType() {
			return CrowdHammerConfiguration.class;
		}
		
	}
	
}
