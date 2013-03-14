package com.adamroughton.consentus;

import java.util.Map;

import org.apache.commons.cli.Option;

public interface CommandLineConfiguration<T> {

	Iterable<Option> getCommandLineOptions();
	
	void configure(final T dependant, 
			final Map<String, String> cmdLineValues);
}
