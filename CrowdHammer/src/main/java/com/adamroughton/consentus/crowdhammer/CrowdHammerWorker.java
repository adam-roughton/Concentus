package com.adamroughton.consentus.crowdhammer;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;

import com.adamroughton.consentus.CommandLineConfiguration;
import com.adamroughton.consentus.ConsentusProcess;
import com.adamroughton.consentus.ConsentusProcessCallback;
import com.adamroughton.consentus.ConsentusProcessConfiguration;
import com.adamroughton.consentus.ConsentusProcessConfiguration.ClusterFactory;
import com.adamroughton.consentus.ConsentusService;
import com.adamroughton.consentus.DefaultProcessCallback;
import com.adamroughton.consentus.Util;
import com.adamroughton.consentus.cluster.worker.ClusterWorker;
import com.adamroughton.consentus.crowdhammer.config.CrowdHammerConfiguration;
import com.adamroughton.consentus.crowdhammer.worker.WorkerService;
import com.adamroughton.consentus.crowdhammer.worker.WorkerServiceConfiguration;
import com.esotericsoftware.minlog.Log;

public final class CrowdHammerWorker implements ConsentusProcess<ClusterWorker, CrowdHammerConfiguration>, 
		ClusterFactory<ClusterWorker> {

	public static final String SERVICE_CLASS_OPTION = "s";
	public static final String PROCESS_NAME = "CrowdHammer Worker";
	
	private final ExecutorService _executor = Executors.newCachedThreadPool();
	private final CrowdHammerService _crowdHammerService;
	
	private ClusterWorker _cluster;
	
	public CrowdHammerWorker(final CrowdHammerService service) {
		_crowdHammerService = Objects.requireNonNull(service);
	}
	
	public void execute() {
		try (ClusterWorker cluster = _cluster) {
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

	@Override
	public void configure(ClusterWorker cluster, CrowdHammerConfiguration config,
			ConsentusProcessCallback exHandler, InetAddress networkAddress) {
		_cluster = cluster;
		_crowdHammerService.configure(config, exHandler, networkAddress);		
	}

	@Override
	public String name() {
		return PROCESS_NAME;
	}
	
	@Override
	public ClusterWorker createCluster(String zooKeeperAddress,
			String zooKeeperRoot, ConsentusProcessCallback callback) {
		return new ClusterWorker(zooKeeperAddress, zooKeeperRoot, _crowdHammerService, _executor, callback);
	}
	
	/*
	 * Static members
	 */
	
	public static void main(String[] args) {
		Options cliOptions = new Options();
		cliOptions.addOption(getServiceOption());
		
		// figure out what the service class is before deciding the type of options we need
		// to check for
		Map<String, String> commandLineArgs = 
				Util.parseCommandLine(PROCESS_NAME, Arrays.asList(getServiceOption()), args, true);
		String serviceClassName = commandLineArgs.get(SERVICE_CLASS_OPTION);
		Class<?> serviceClass = getServiceClass(serviceClassName);
		
		final CrowdHammerService crowdHammerService = createCrowdHammerService(serviceClassName);
		final CrowdHammerWorker worker = new CrowdHammerWorker(crowdHammerService);
		
		Map<CommandLineConfiguration<?>, ConfigurationOperation> commandLineConfigurations = new HashMap<>();
		
		// add base configuration
		commandLineConfigurations.put(
				new ConsentusProcessConfiguration<>(worker, CrowdHammerConfiguration.class, new DefaultProcessCallback()), 
				new ConfigurationOperation() {
					
					@Override
					public void doOperation(CommandLineConfiguration<?> config,
							Map<String, String> configMappings) {
						@SuppressWarnings("unchecked")
						ConsentusProcessConfiguration<ClusterWorker, CrowdHammerConfiguration> baseConfig
							= (ConsentusProcessConfiguration<ClusterWorker, CrowdHammerConfiguration>) config;
						baseConfig.configure(worker, configMappings);
					}
					
				});
		
		// get additional command line configurations
		if (WorkerService.class.isAssignableFrom(serviceClass)) {
			commandLineConfigurations.put(new WorkerServiceConfiguration(), new ConfigurationOperation() {
				
				@Override
				public void doOperation(CommandLineConfiguration<?> configuration,
						Map<String, String> configMappings) {
					WorkerServiceConfiguration workerConfig = (WorkerServiceConfiguration) configuration;
					WorkerService service = (WorkerService) crowdHammerService;
					workerConfig.configure(service, configMappings);
				}
				
			});
			
		}
		
		// get all of the required command line options
		List<Option> options = new ArrayList<>();
		for (CommandLineConfiguration<?> config : commandLineConfigurations.keySet()) {
			for (Option opt : config.getCommandLineOptions()) {
				options.add(opt);
			}			
		}
		
		commandLineArgs = Util.parseCommandLine(PROCESS_NAME, options, args, true);
		
		// perform configurations
		for (Entry<CommandLineConfiguration<?>, ConfigurationOperation> entry : commandLineConfigurations.entrySet()) {
			entry.getValue().doOperation(entry.getKey(), commandLineArgs);
		}
		
		// execute
		worker.execute();
	}
	
	private static Class<?> getServiceClass(String serviceClassName) {
		try {
			return Class.forName(serviceClassName);
		} catch (ClassNotFoundException eNotFound){
			throw new RuntimeException(String.format("Could not find the service class '%1$s'.", serviceClassName), eNotFound);
		} 
	}
	
	@SuppressWarnings("unchecked")
	private static CrowdHammerService createCrowdHammerService(final String serviceClassName) {
		try {
			Class<?> clazz = getServiceClass(serviceClassName);
			if (ConsentusService.class.isAssignableFrom(clazz)) {
				Class<? extends ConsentusService> consentusServiceClass = 
						(Class<? extends ConsentusService>) clazz;
				return new ConsentusServiceAdapter(consentusServiceClass);
			} else if (CrowdHammerService.class.isAssignableFrom(clazz)) {
				Class<? extends CrowdHammerService> crowdHammerServiceClass = 
						(Class<? extends CrowdHammerService>) clazz;
				return crowdHammerServiceClass.newInstance();
			} else {
				throw new RuntimeException(String.format("The provided service class was not of type '%1$s' or '%2$s'.", 
						ConsentusService.class.getName(),
						CrowdHammerService.class.getName()));
			}
		} catch (InstantiationException | IllegalAccessException | SecurityException e) {
			throw new RuntimeException(String.format("Could not instantiate service class %1$s."), e);
		}
	}
	
	private interface ConfigurationOperation {
		void doOperation(final CommandLineConfiguration<?> configuration, 
				final Map<String, String> configMappings);
	}
	
	@SuppressWarnings("static-access")
	private static Option getServiceOption() {
		return OptionBuilder.withArgName("service class")
				.hasArgs()
				.isRequired(true)
				.withDescription("fully qualified class name of the service class.")
				.create(SERVICE_CLASS_OPTION);
	}
	
}
