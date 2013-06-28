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

import static com.adamroughton.concentus.Constants.METRIC_BUFFER_SECONDS;

import java.net.InetAddress;
import java.util.HashMap;
import java.util.ListIterator;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import com.adamroughton.concentus.cluster.coordinator.ClusterCoordinatorHandle;
import com.adamroughton.concentus.cluster.worker.ClusterListener;
import com.adamroughton.concentus.cluster.worker.ClusterStateValue;
import com.adamroughton.concentus.cluster.worker.ClusterWorkerContainer;
import com.adamroughton.concentus.config.Configuration;
import com.adamroughton.concentus.metric.LogMetricContext;
import com.adamroughton.concentus.metric.MetricContext;
import com.adamroughton.concentus.metric.NullMetricContext;
import com.esotericsoftware.minlog.Log;

public class ConcentusExecutableOperations {
	
	public static <TConfig extends Configuration, TClusterState extends Enum<TClusterState> & ClusterStateValue> 
			void executeClusterWorker(String[] args, ConcentusWorkerNode<TConfig, TClusterState> workerNode) {
		
		Clock clock = new DefaultClock();
		
		Map<String, String> commandLineArgs = parseCommandLineForNode(args, workerNode);	
		TConfig config = SharedCommandLineOptions.readConfig(workerNode.getConfigType(), commandLineArgs);
		String zooKeeperAddress = SharedCommandLineOptions.readZooKeeperAddress(commandLineArgs);
		InetAddress nodeAddress = SharedCommandLineOptions.readNodeAddress(commandLineArgs);
		boolean useTracingComponents = SharedCommandLineOptions.readTraceOption(commandLineArgs);
		
		long metricBufferMillis = TimeUnit.SECONDS.toMillis(METRIC_BUFFER_SECONDS) * 5;
		MetricContext metricContext = new LogMetricContext(Constants.METRIC_TICK, metricBufferMillis, clock);
		metricContext.start();
		
		ConcentusHandle<TConfig> concentusHandle = ConcentusHandleFactory.createHandle(clock, 
				config, zooKeeperAddress, nodeAddress, metricContext, useTracingComponents);
		
		ClusterListener<TClusterState> concentusWorkerService = workerNode.createService(commandLineArgs, concentusHandle, metricContext);
		
		ExecutorService executor = Executors.newCachedThreadPool();
		
		try (ClusterWorkerContainer cluster = new ClusterWorkerContainer(
				concentusHandle.getZooKeeperAddress(), 
				concentusHandle.getConfig().getZooKeeper().getAppRoot(), 
				concentusWorkerService, 
				executor, 
				concentusHandle)) {
			cluster.start();
			
			// Wait for exit
			Object waitMonitor = new Object();
			synchronized (waitMonitor) {
				waitMonitor.wait();
			}
			
		} catch (Exception e) {
			Log.error("Error thrown from the cluster participant", e);
		}
	}
	
	public static <TConfig extends Configuration> void executeClusterCoordinator(
			String[] args, ConcentusCoordinatorNode<TConfig> coordinatorNode) {
		Clock clock = new DefaultClock();
		
		Map<String, String> commandLineArgs = parseCommandLineForNode(args, coordinatorNode);
		TConfig config = SharedCommandLineOptions.readConfig(coordinatorNode.getConfigType(), commandLineArgs);
		String zooKeeperAddress = SharedCommandLineOptions.readZooKeeperAddress(commandLineArgs);
		InetAddress nodeAddress = SharedCommandLineOptions.readNodeAddress(commandLineArgs);
		boolean useTracingComponents = SharedCommandLineOptions.readTraceOption(commandLineArgs);
		
		MetricContext metricContext = new NullMetricContext();
		
		ConcentusHandle<TConfig> concentusHandle = ConcentusHandleFactory.createHandle(clock, 
				config, zooKeeperAddress, nodeAddress, metricContext, useTracingComponents);
		ClusterCoordinatorHandle coordinatorClusterHandle = new ClusterCoordinatorHandle(
				concentusHandle.getZooKeeperAddress(), 
				concentusHandle.getConfig().getZooKeeper().getAppRoot(), 
				concentusHandle);
		coordinatorNode.run(commandLineArgs, concentusHandle, coordinatorClusterHandle);
	}
	
	private static Map<String, String> parseCommandLineForNode(String[] args, ConcentusNode<?> node) {
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
	
	public static Map<String, String> parseCommandLine(String processName, Iterable<Option> options, String[] args, boolean ignoreUnknownOptions) {
		Options cliOptions = new Options();
		for (Option option : options) {
			cliOptions.addOption(option);
		}
		return parseCommandLine(processName, cliOptions, args, ignoreUnknownOptions);
	}
	
	public static Map<String, String> parseCommandLine(String processName, Options cliOptions, String[] args, boolean ignoreUnknownOptions) { 
		Map<String, String> parsedCommandLine = new HashMap<>();
		CommandLineParser parser = new TolerantParser(ignoreUnknownOptions);
		try {
			CommandLine commandLine = parser.parse(cliOptions, args);
			for (Object option : cliOptions.getOptions()) {
				String opt = ((Option) option).getOpt();
				if (commandLine.hasOption(opt)) {
					if (((Option) option).hasArgs()) {
						parsedCommandLine.put(opt, commandLine.getOptionValue(opt).trim());
					} else {
						parsedCommandLine.put(opt, "");
					}
				}
			}
		} catch (ParseException eParse) {
			HelpFormatter helpFormatter = new HelpFormatter();
			helpFormatter.printHelp(String.format("%s [options]", processName), cliOptions);
			System.exit(1);
		}
		return parsedCommandLine;
	}
	
	private static class TolerantParser extends GnuParser {

		private final boolean _ignoreUnrecognisedOptions;
		
		public TolerantParser(boolean ignoreUnrecognisedOptions) {
			_ignoreUnrecognisedOptions = ignoreUnrecognisedOptions;
		}

		@SuppressWarnings("rawtypes")
		@Override
		protected void processOption(String arg, ListIterator iter)
				throws ParseException {
			if (getOptions().hasOption(arg) || !_ignoreUnrecognisedOptions) {
				super.processOption(arg, iter);
			}
		}
	}
	
	public interface FactoryDelegate<TProcess, TConfig extends Configuration> {
		TProcess create(ConcentusHandle<? extends TConfig> concentusHandle);
	}
	
}
