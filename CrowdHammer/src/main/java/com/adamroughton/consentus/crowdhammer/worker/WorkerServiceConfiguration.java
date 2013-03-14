package com.adamroughton.consentus.crowdhammer.worker;

import java.util.Arrays;
import java.util.Map;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;

import com.adamroughton.consentus.CommandLineConfiguration;

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
