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

import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;

import com.adamroughton.consentus.ConsentusProcessCallback;
import com.adamroughton.consentus.ConsentusService;
import com.esotericsoftware.minlog.Log;

public class CrowdHammerProcess implements ConsentusProcessCallback {
	
	private final static char SERVICE_CLASS_OPTION = 's';
	private final static char PROPERTIES_FILE_OPTION = 'p';
	
	private final AtomicBoolean _hasStarted;
	private final AtomicBoolean _isShuttingDown;
	private final ConsentusService _service;
	private final TestConfig _conf;
	
	public CrowdHammerProcess(ConsentusService service) {
		_service = service;
		_hasStarted = new AtomicBoolean(false);
		_isShuttingDown = new AtomicBoolean(false);
		
		//TODO load from file
		_conf = new TestConfig();
		_conf.setWorkingDir(".");
		_conf.setCanonicalSubPort("9000");
		_conf.setCanonicalStatePubPort("9001");
		_conf.setTestMetricSubPort("10000");
		_conf.setTestClass("com.adamroughton.consentus.");
		
		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override
			public void run() {
				Log.info("Shutting Down");
				doShutdown();
			}	
		});
	}

	public void start() {
		if (!_hasStarted.getAndSet(true)) {
			_service.start(_conf, this);
		}
	}

	@Override
	public void signalFatalException(Throwable ex) {
		Log.error(String.format("Error in service: %s", _service.name()), ex);
		System.exit(1);
	}

	@Override
	public void shutdown() {
		System.exit(0);
	}
	
	private void doShutdown() {
		if (!_isShuttingDown.getAndSet(true)) {
			_service.shutdown();
		}
	}
	
	@SuppressWarnings("static-access")
	private static Options getCommandLineOptions() {
		Options cliOptions = new Options();
		
		Option testClassOption = OptionBuilder.withArgName("service class")
				   .hasArgs()
				   .isRequired(true)
				   .withDescription("fully qualified class name of the service class")
				   .create(SERVICE_CLASS_OPTION);
		Option propertiesFileOption = OptionBuilder.withArgName("file path")
				   .hasArgs()
				   .isRequired(false)
				   .withDescription("path to the properties file")
				   .create(PROPERTIES_FILE_OPTION);
		
		cliOptions.addOption(testClassOption);
		cliOptions.addOption(propertiesFileOption);
		return cliOptions;
	}
	
	@SuppressWarnings("unchecked")
	private static ConsentusService getTestEntryFactory(final String serviceClassName) {
		Class<? extends ConsentusService> serviceClass;
		try {
			Class<?> clazz = Class.forName(serviceClassName);
			if (ConsentusService.class.isAssignableFrom(clazz)) {
				serviceClass = (Class<? extends ConsentusService>) clazz;
			} else {
				throw new RuntimeException(String.format("Provided service class was not of type '%1$s'.", ConsentusService.class.getName()));
			}
		} catch (ClassNotFoundException eNotFound){
			throw new RuntimeException(String.format("Could not find the service class '%1$s'.", serviceClassName), eNotFound);
		}
		try {
			return serviceClass.newInstance();
		} catch (InstantiationException | IllegalAccessException | SecurityException e) {
			throw new RuntimeException(String.format("Could not instantiate service class %1$s."), e);
		}
	}
	
	public static void main(String[] args) {
		// zookeeper address
		// configuration file
		
		Options cliOptions = getCommandLineOptions();
		
		ConsentusService service = null;
		try {
			CommandLineParser parser = new GnuParser();
			CommandLine commandLine = parser.parse(cliOptions, args);
			service = getTestEntryFactory(commandLine.getOptionValue(SERVICE_CLASS_OPTION));
		} catch (Exception e) {
			e.printStackTrace();
			HelpFormatter formatter = new HelpFormatter();
			formatter.printHelp("consentus [OPTIONS]", cliOptions);
			System.exit(1);
		}
		try {
			CrowdHammerProcess process = new CrowdHammerProcess(service);
			process.start();
			final Object foreverWait = new Object();
			synchronized (foreverWait) {
				foreverWait.wait();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}		
	}
}
