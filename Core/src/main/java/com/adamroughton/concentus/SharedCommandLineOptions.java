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
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;

public class SharedCommandLineOptions {

	public final static String ZOOKEEPER_ADDRESS_OPTION = "zkaddr";
	public final static String ZOOKEEPER_APP_ROOT_OPTION = "zkroot";
	public final static String NETWORK_ADDRESS_OPTION = "hostaddr";
	public final static String TRACE_OPTION = "trace";
	
	@SuppressWarnings("static-access")
	public static Iterable<Option> getCommandLineOptions() {
		return Arrays.asList(
				OptionBuilder.withArgName("ZooKeeper Address")
					.hasArg()
					.isRequired(true)
					.withDescription("the address of the ZooKeeper server")
					.create(ZOOKEEPER_ADDRESS_OPTION),
				OptionBuilder.withArgName("ZooKeeper Application Root")
					.hasArg()
					.isRequired(false)
					.withDescription("override the default app root '" + Constants.DEFAULT_ZOO_KEEPER_ROOT + "'")
					.create(ZOOKEEPER_APP_ROOT_OPTION),
				OptionBuilder.withArgName("network address")
					.hasArg()
					.isRequired(true)
					.withDescription("address to bind sockets to")
					.create(NETWORK_ADDRESS_OPTION),
				OptionBuilder.withArgName("trace")
					.hasArgs()
					.withValueSeparator(' ')
					.isRequired(false)
					.withDescription("trace selected components: -t <component1> <component2> (available: messengers, queues)")
					.create(TRACE_OPTION)
			);
	}
	
	public static String readZooKeeperAddress(Map<String, String> cmdLineValues) {
		return cmdLineValues.get(ZOOKEEPER_ADDRESS_OPTION);
	}
	
	public static String getZooKeeperAppRoot(Map<String, String> cmdLineValues) {
		if (cmdLineValues.containsKey(ZOOKEEPER_APP_ROOT_OPTION)) {
			return cmdLineValues.get(ZOOKEEPER_APP_ROOT_OPTION);
		} else {
			return Constants.DEFAULT_ZOO_KEEPER_ROOT;
		}
	}
	
	public static InetAddress readNodeAddress(Map<String, String> cmdLineValues) {
		String addressString = cmdLineValues.get(NETWORK_ADDRESS_OPTION);
		try {
			return InetAddress.getByName(addressString);
		} catch (UnknownHostException eBadIP) {
			throw new RuntimeException(String.format("IP address '%s' is not address for this host.", addressString));
		}
	}
	
	public static Set<String> readTraceOption(Map<String, String> cmdLineValues) {
		String traceArrayString = cmdLineValues.get(TRACE_OPTION);
		if (traceArrayString == null) {
			traceArrayString = "";
		}
		String[] traceOptions = traceArrayString.split(",");
		Set<String> traceFlagSet = new HashSet<>(traceOptions.length);
		for (String traceOption : traceOptions) {
			traceFlagSet.add(traceOption);
		}
		return traceFlagSet;
	}

}
