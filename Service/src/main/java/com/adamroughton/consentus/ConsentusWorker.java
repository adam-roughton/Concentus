package com.adamroughton.consentus;

import java.net.InetAddress;
import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;

import com.adamroughton.consentus.ConsentusProcessCallback;
import com.adamroughton.consentus.ConsentusProcessConfiguration.ClusterFactory;
import com.adamroughton.consentus.ConsentusService;
import com.adamroughton.consentus.DefaultProcessCallback;
import com.adamroughton.consentus.cluster.worker.ClusterWorker;
import com.adamroughton.consentus.config.Configuration;
import com.esotericsoftware.minlog.Log;

public final class ConsentusWorker implements ConsentusProcess<ClusterWorker, Configuration>, 
		ClusterFactory<ClusterWorker> {

	public static final String SERVICE_CLASS_OPTION = "s";
	
	private final ExecutorService _executor = Executors.newCachedThreadPool();
	private ConsentusService _service;
	private ClusterWorker _cluster;
	
	public ConsentusWorker(final ConsentusService service) {
		_service = Objects.requireNonNull(service);
	}

	@Override
	public void configure(ClusterWorker cluster, Configuration config,
			ConsentusProcessCallback exHandler, InetAddress networkAddress) {
		_cluster = cluster;
		_service.configure(config, exHandler, networkAddress);
	}

	@Override
	public String name() {
		return _service.name();
	}

	@Override
	public void execute() throws InterruptedException {
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
	public ClusterWorker createCluster(String zooKeeperAddress,
			String zooKeeperRoot, ConsentusProcessCallback callback) {
		return new ClusterWorker(zooKeeperAddress, zooKeeperRoot, _service, _executor, callback);
	}
	
	public static void main(String[] args) {
		Map<String, String> cmdLineValues = Util.parseCommandLine(
				"Consentus Worker", Arrays.asList(getServiceOption()), args, true);
		
		String serviceClassName = cmdLineValues.get(SERVICE_CLASS_OPTION);
		ConsentusService service = createConsentusService(serviceClassName);
		
		ConsentusWorker worker = new ConsentusWorker(service);
		
		ConsentusProcessConfiguration<ClusterWorker, Configuration> baseConfig = 
				new ConsentusProcessConfiguration<>(worker, Configuration.class, new DefaultProcessCallback());
		
		cmdLineValues = Util.parseCommandLine(service.name(), baseConfig, args);
		baseConfig.configure(worker, cmdLineValues);
		try {
			worker.execute();
		} catch (Exception e) {
			Log.error("Error while running:", e);
			System.exit(1);
		}
	}
	
	@SuppressWarnings("unchecked")
	private static ConsentusService createConsentusService(final String serviceClassName) {
		try {
			Class<?> clazz = Class.forName(serviceClassName);
			if (ConsentusService.class.isAssignableFrom(clazz)) {
				Class<? extends ConsentusService> consentusServiceClass = 
						(Class<? extends ConsentusService>) clazz;
				return consentusServiceClass.newInstance();
			} else {
				throw new RuntimeException(String.format("The provided service class was not of type '%1$s'.", ConsentusService.class.getName()));
			}
		} catch (ClassNotFoundException eNotFound){
			throw new RuntimeException(String.format("Could not find the service class '%1$s'.", serviceClassName), eNotFound);
		} catch (InstantiationException | IllegalAccessException | SecurityException e) {
			throw new RuntimeException(String.format("Could not instantiate service class %1$s."), e);
		}
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
