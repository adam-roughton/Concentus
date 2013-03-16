package com.adamroughton.consentus;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Map;
import java.util.Objects;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;

import com.adamroughton.consentus.cluster.ClusterParticipant;
import com.adamroughton.consentus.config.Configuration;

public class ConsentusProcessConfiguration<TCluster extends ClusterParticipant, TConfig extends Configuration> 
		implements CommandLineConfiguration<ConsentusProcess<TCluster, TConfig>> {

	public final static String ZOOKEEPER_ADDRESS_OPTION = "z";
	public final static String PROPERTIES_FILE_OPTION = "p";
	public final static String NETWORK_ADDRESS_OPTION = "a";
	
	private final ClusterFactory<TCluster> _clusterFactory;
	private final Class<TConfig> _configType;
	private final ConsentusProcessCallback _callback;
	
	public ConsentusProcessConfiguration(
			final ClusterFactory<TCluster> clusterFactory,
			final Class<TConfig> configType, 
			final ConsentusProcessCallback callback) {
		_clusterFactory = Objects.requireNonNull(clusterFactory);
		_configType = Objects.requireNonNull(configType);
		_callback = Objects.requireNonNull(callback);
	}
	
	@SuppressWarnings("static-access")
	@Override
	public Iterable<Option> getCommandLineOptions() {
		return Arrays.asList(
				OptionBuilder.withArgName("ZooKeeper Address")
					.hasArgs()
					.isRequired(true)
					.withDescription("the address of the ZooKeeper server")
					.create(ZOOKEEPER_ADDRESS_OPTION),
				OptionBuilder.withArgName("file path")
					.hasArgs()
					.isRequired(true)
					.withDescription("path to the properties file")
					.create(PROPERTIES_FILE_OPTION),
				OptionBuilder.withArgName("network address")
					.hasArgs()
					.isRequired(true)
					.withDescription("address to bind sockets to")
					.create(NETWORK_ADDRESS_OPTION)
			);
	}

	@Override
	public void configure(ConsentusProcess<TCluster, TConfig> process,
			Map<String, String> cmdLineValues) {
		String configPath = cmdLineValues.get(PROPERTIES_FILE_OPTION);
		TConfig config = Util.readConfig(_configType, configPath);
		
		String zooKeeperAddress = cmdLineValues.get(ZOOKEEPER_ADDRESS_OPTION);
		
		String addressString = cmdLineValues.get(NETWORK_ADDRESS_OPTION);
		InetAddress networkAddress;
		try {
			networkAddress = InetAddress.getByName(addressString);
		} catch (UnknownHostException eBadIP) {
			throw new RuntimeException(String.format("IP address '%s' is not address for this host.", addressString));
		}
		
		String zooKeeperRoot = config.getZooKeeper().getAppRoot();
		//TODO move validation into configuration class
		if (!Util.isValidZKRoot(zooKeeperRoot)) {
			throw new RuntimeException(
					String.format("The ZooKeeper App Root '%s' was not a valid root path " +
							"(can be '/' or '/[A-Za-z0-9]+')", zooKeeperRoot));
		}
		TCluster cluster = _clusterFactory.createCluster(zooKeeperAddress, zooKeeperRoot, _callback);		
		process.configure(cluster, config, _callback, networkAddress);
	}
	
	public interface ClusterFactory<TCluster extends ClusterParticipant> {
		
		TCluster createCluster(final String zooKeeperAddress, final String zooKeeperRoot, final ConsentusProcessCallback callback);
		
	}

}