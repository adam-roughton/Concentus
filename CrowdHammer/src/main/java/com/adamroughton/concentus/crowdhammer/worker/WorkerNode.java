package com.adamroughton.concentus.crowdhammer.worker;

import java.util.Arrays;
import java.util.Map;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;

import com.adamroughton.concentus.ConcentusExecutableOperations;
import com.adamroughton.concentus.ConcentusHandle;
import com.adamroughton.concentus.ConcentusNode;
import com.adamroughton.concentus.ConcentusProcessFactory;
import com.adamroughton.concentus.crowdhammer.config.CrowdHammerConfiguration;

public class WorkerNode implements ConcentusNode<WorkerService, CrowdHammerConfiguration> {
	
	@Override
	public ConcentusProcessFactory<WorkerService, CrowdHammerConfiguration> getProcessFactory() {
		return new WorkerServiceFactory();
	}
	
	public static void main(String[] args) {
		ConcentusExecutableOperations.executeClusterWorker(args, new WorkerNode());
	}

	public static class WorkerServiceFactory implements ConcentusProcessFactory<WorkerService, CrowdHammerConfiguration> {
	
		public static final String MAX_CLIENT_COUNT_OPTION = "n";
	
		@Override
		public WorkerService create(
				ConcentusHandle<? extends CrowdHammerConfiguration> concentusHandle,
				Map<String, String> commandLineOptions) {
			int maxClientCount = Integer.parseInt(commandLineOptions.get(MAX_CLIENT_COUNT_OPTION));
			return new WorkerService(concentusHandle, maxClientCount);
		}
	
		@Override
		@SuppressWarnings("static-access")
		public Iterable<Option> getCommandLineOptions() {
			return Arrays.asList(OptionBuilder.withArgName("max client count")
					.hasArgs()
					.isRequired(true)
					.withDescription("Maximum number of similated clients to run on this worker.")
					.create(MAX_CLIENT_COUNT_OPTION));
		}		
	
		@Override
		public String getProcessName() {
			return "CrowdHammer Worker";
		}
	
		@Override
		public Class<WorkerService> getSupportedType() {
			return WorkerService.class;
		}

		@Override
		public Class<CrowdHammerConfiguration> getConfigType() {
			return CrowdHammerConfiguration.class;
		}
	
	}


}
