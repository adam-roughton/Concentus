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
import com.adamroughton.concentus.util.Util;

public class SharedCommandLineOptions {

	public final static String ZOOKEEPER_ADDRESS_OPTION = "z";
	public final static String PROPERTIES_FILE_OPTION = "p";
	public final static String NETWORK_ADDRESS_OPTION = "a";
	public final static String TRACE_OPTION = "t";
	
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
					.create(NETWORK_ADDRESS_OPTION),
				OptionBuilder.withArgName("trace")
					.hasArg(false)
					.isRequired(false)
					.withDescription("wraps select components with tracing versions that output to stdout")
					.create(TRACE_OPTION)
			);
	}
	
	public static <TConfig extends Configuration> TConfig readConfig(Class<TConfig> configType, Map<String, String> cmdLineValues) {
		String configPath = cmdLineValues.get(PROPERTIES_FILE_OPTION);
		return Util.readConfig(configType, configPath);
	}
	
	public static String readZooKeeperAddress(Map<String, String> cmdLineValues) {
		return cmdLineValues.get(ZOOKEEPER_ADDRESS_OPTION);
	}
	
	public static InetAddress readNodeAddress(Map<String, String> cmdLineValues) {
		String addressString = cmdLineValues.get(NETWORK_ADDRESS_OPTION);
		try {
			return InetAddress.getByName(addressString);
		} catch (UnknownHostException eBadIP) {
			throw new RuntimeException(String.format("IP address '%s' is not address for this host.", addressString));
		}
	}
	
	public static boolean readTraceOption(Map<String, String> cmdLineValues) {
		return cmdLineValues.containsKey(TRACE_OPTION);
	}

}
