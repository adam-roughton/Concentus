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
