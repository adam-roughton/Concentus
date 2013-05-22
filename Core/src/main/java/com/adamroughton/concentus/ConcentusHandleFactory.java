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
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Map;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;

import com.adamroughton.concentus.config.Configuration;
import com.adamroughton.concentus.messaging.zmq.SocketManager;
import com.adamroughton.concentus.messaging.zmq.SocketManagerImpl;
import com.adamroughton.concentus.messaging.zmq.TrackingSocketManagerDecorator;
import com.adamroughton.concentus.util.Util;

public class ConcentusHandleFactory {

	public final static String ZOOKEEPER_ADDRESS_OPTION = "z";
	public final static String PROPERTIES_FILE_OPTION = "p";
	public final static String NETWORK_ADDRESS_OPTION = "a";
	
	@SuppressWarnings("static-access")
	public static Iterable<Option> getCommandLineOptions() {
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

	public static <TConfig extends Configuration> ConcentusHandle<TConfig> createHandle(Class<TConfig> configType, Map<String, String> cmdLineValues) {
		final Clock clock = new DefaultClock();
		
		InstanceFactory<SocketManager> socketManagerFactory = new InstanceFactory<SocketManager>() {
			
			@Override
			public SocketManager newInstance() {
				return new TrackingSocketManagerDecorator(new SocketManagerImpl(clock), clock);
			}
		};
		
		String configPath = cmdLineValues.get(PROPERTIES_FILE_OPTION);
		TConfig config = Util.readConfig(configType, configPath);
		
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
		return new ConcentusHandle<TConfig>(socketManagerFactory, clock, config, networkAddress, zooKeeperAddress);
	}

}
