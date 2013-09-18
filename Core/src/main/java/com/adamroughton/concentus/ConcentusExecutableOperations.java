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
package com.adamroughton.concentus;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.javatuples.Pair;

import com.adamroughton.concentus.cluster.ClusterParticipant;
import com.adamroughton.concentus.cluster.worker.ClusterHandle;
import com.adamroughton.concentus.cluster.worker.ServiceContainer;
import com.adamroughton.concentus.cluster.worker.ServiceDeployment;
import com.adamroughton.concentus.data.ResizingBuffer;
import com.adamroughton.concentus.data.cluster.kryo.ClusterState;
import com.adamroughton.concentus.util.Util;

public class ConcentusExecutableOperations {
	
	public static interface ClusterHandleFactory<TClusterHandle extends ClusterParticipant> {
		TClusterHandle create(String zooKeeperAddress, 
				String root,
				UUID clusterId,
				FatalExceptionCallback exHandler);
	}
	
	public static <TState extends Enum<TState> & ClusterState> void executeClusterService(String[] args, ServiceDeployment<TState> serviceDeployment, 
			ComponentResolver<? extends ResizingBuffer> componentResolver) throws Exception {
		Pair<ClusterHandle, ConcentusHandle> coreComponents = createCoreComponents("ClusterService", args, 
				new ClusterHandleFactory<ClusterHandle>() {

					@Override
					public ClusterHandle create(String zooKeeperAddress,
							String root, UUID clusterId,
							FatalExceptionCallback exHandler) {
						return new ClusterHandle(zooKeeperAddress, root, clusterId, exHandler);
					}
			
				});

		ClusterHandle clusterHandle = coreComponents.getValue0();
		ConcentusHandle concentusHandle = coreComponents.getValue1();
		
		try (ServiceContainer<TState> container = new ServiceContainer<>(concentusHandle, clusterHandle, serviceDeployment, 
				componentResolver, concentusHandle)) {
			container.start();
			
			// Wait for exit
			Object waitMonitor = new Object();
			synchronized (waitMonitor) {
				waitMonitor.wait();
			}	
		}
	}
	
	public static <TClusterHandle extends ClusterParticipant> Pair<TClusterHandle, ConcentusHandle> 
			createCoreComponents(String name, String[] args, ClusterHandleFactory<TClusterHandle> clusterHandleFactory) {
		Map<String, String> commandLineArgs = parseCommandLine("ClusterService", SharedCommandLineOptions.getCommandLineOptions(), args, false);
		
		String zooKeeperAddress = SharedCommandLineOptions.readZooKeeperAddress(commandLineArgs);
		String zooKeeperAppRoot = SharedCommandLineOptions.getZooKeeperAppRoot(commandLineArgs);
		InetAddress hostAddress = SharedCommandLineOptions.readNodeAddress(commandLineArgs);
		Set<String> traceFlagSet = SharedCommandLineOptions.readTraceOption(commandLineArgs);
		
		Clock clock = new DefaultClock();
		
		if (!Util.isValidZKRoot(zooKeeperAppRoot)) {
			throw new RuntimeException(
					String.format("The ZooKeeper App Root '%s' was not a valid root path " +
							"(can be '/' or '/[A-Za-z0-9]+')", zooKeeperAppRoot));
		}	
		ConcentusHandle concentusHandle = new ConcentusHandle(clock, hostAddress, zooKeeperAddress, traceFlagSet);
		UUID clusterId = UUID.randomUUID();
		TClusterHandle clusterHandle = clusterHandleFactory.create(zooKeeperAddress, zooKeeperAppRoot, clusterId, concentusHandle);
		
		return new Pair<>(clusterHandle, concentusHandle);
	}
		
	public static Map<String, String> parseCommandLineForNode(String[] args, ConcentusProcess node) {
		Options cliOptions = new Options();
		addTo(cliOptions, SharedCommandLineOptions.getCommandLineOptions());
		addTo(cliOptions, node.getCommandLineOptions());
		return parseCommandLine(node.getProcessName(), cliOptions, args, false);
	}
	
	public static void addTo(Options options, Iterable<Option> optionSet) {
		for (Option option : optionSet) {
			options.addOption(option);
		}
	}
	
	public static Map<String, String> parseCommandLine(String processName, Iterable<Option> options, 
			String[] args, boolean ignoreUnknownOptions) {
		return parseCommandLine(processName, toOptions(options), args, ignoreUnknownOptions);
	}
	
	public static Map<String, String> parseCommandLine(String processName, Options cliOptions, 
			String[] args, boolean ignoreUnknownOptions) { 
		return parseCommandLineInternal(processName, cliOptions, args, ignoreUnknownOptions).getValue0();
	}
	
	public static Pair<Map<String, String>, String[]> parseCommandLineKeepRemaining(String processName, 
			Iterable<Option> options, String[] args, boolean ignoreUnknownOptions) {
		return parseCommandLineKeepRemaining(processName, toOptions(options), args, ignoreUnknownOptions);
	}
	
	public static Pair<Map<String, String>, String[]> parseCommandLineKeepRemaining(String processName, 
			Options cliOptions, String[] args, boolean ignoreUnknownOptions) {
		return parseCommandLineInternal(processName, cliOptions, args, ignoreUnknownOptions);
	}
	
	private static Options toOptions(Iterable<Option> options) {
		Options cliOptions = new Options();
		for (Option option : options) {
			cliOptions.addOption(option);
		}
		return cliOptions;
	}
	
	private static Pair<Map<String, String>, String[]> parseCommandLineInternal(String processName, Options cliOptions, String[] args, 
			boolean ignoreUnknownOptions) { 
		Map<String, String> parsedCommandLine = new HashMap<>();
		TolerantParser parser = new TolerantParser(ignoreUnknownOptions);
		String[] leftOverArgs = new String[0];
		try {
			CommandLine commandLine = parser.parse(cliOptions, args);
			for (Object optionObjRef : cliOptions.getOptions()) {
				Option option = (Option) optionObjRef;
				String opt = option.getOpt();
				if (commandLine.hasOption(opt)) {
					if (option.hasArgs()) {
						String[] arguments = commandLine.getOptionValues(opt);
						StringBuilder builder = new StringBuilder();
						for (int i = 0; i < arguments.length; i++) {
							if (i > 0) {
								builder.append(",");
							}
							builder.append(arguments[i].trim());
						}
						parsedCommandLine.put(opt, builder.toString());
					} else if (option.hasArg()) {
						parsedCommandLine.put(opt, commandLine.getOptionValue(opt).trim());
					} else {
						parsedCommandLine.put(opt, "");
					}
				}
			}
			leftOverArgs = parser.getUnrecognisedArgs();
		} catch (ParseException eParse) {
			HelpFormatter helpFormatter = new HelpFormatter();
			helpFormatter.printHelp(String.format("%s [options]", processName), cliOptions);
			System.exit(1);
		}
		return new Pair<>(parsedCommandLine, leftOverArgs);
	}
	
	private static class TolerantParser extends GnuParser {

		private final boolean _ignoreUnrecognisedOptions;
		private final List<String> _unrecognisedArgs = new ArrayList<>();
		
		public TolerantParser(boolean ignoreUnrecognisedOptions) {
			_ignoreUnrecognisedOptions = ignoreUnrecognisedOptions;
		}

		@SuppressWarnings("rawtypes")
		@Override
		protected void processOption(String arg, ListIterator iter)
				throws ParseException {
			if (getOptions().hasOption(arg) || !_ignoreUnrecognisedOptions) {
				super.processOption(arg, iter);
			} else {
				_unrecognisedArgs.add(arg);
				String nextUnrecogArg;
				while (iter.hasNext() && !getOptions().hasOption((nextUnrecogArg = (String) iter.next()))) {
					_unrecognisedArgs.add(nextUnrecogArg);
				}
				iter.previous();
			}
		}
		
		public String[] getUnrecognisedArgs() {
			return _unrecognisedArgs.toArray(new String[_unrecognisedArgs.size()]);
		}
	}
	
}
