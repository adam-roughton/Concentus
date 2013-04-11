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
package com.adamroughton.consentus;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;
import java.util.regex.Pattern;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.io.Charsets;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;

import com.adamroughton.consentus.config.Configuration;
import com.adamroughton.consentus.messaging.MessageBytesUtil;
import com.adamroughton.consentus.messaging.events.EventType;
import com.esotericsoftware.kryo.Kryo;
import com.lmax.disruptor.EventFactory;

public class Util {
	
	public static void assertPortValid(int port) {
		if (port < 1024 || port > 65535)
			throw new RuntimeException(String.format("Bad port number: %d", port));
	}
	
	public static int getPort(final String portString) {
		int port = Integer.parseInt(portString);
		Util.assertPortValid(port);
		return port;
	}
	
	public static void initialiseKryo(Kryo kryo) {
		for (EventType eventType : EventType.values()) {
			kryo.register(eventType.getEventClass(), eventType.getId());
		}
	}
	
	public static Kryo createKryoInstance() {
		Kryo kryo = new Kryo();
		initialiseKryo(kryo);
		return kryo;
	}
	
	public static byte[] getSubscriptionBytes(EventType eventType) {
		byte[] subId = new byte[4];
		MessageBytesUtil.writeInt(subId, 0, eventType.getId());
		return subId;
	}
	
	public static byte[] intToBytes(int value) {
		byte[] intBytes = new byte[4];
		MessageBytesUtil.writeInt(intBytes, 0, value);
		return intBytes;
	}
	
	public static void writeSubscriptionBytes(EventType eventType, byte[] buffer, int offset) {
		MessageBytesUtil.writeInt(buffer, offset, eventType.getId());
	}
	
	public static EventFactory<byte[]> msgBufferFactory(final int msgBufferSize) {
		return new EventFactory<byte[]>() {
			public byte[] newInstance() {
				return new byte[msgBufferSize];
			}
		};
	}
	
	public static String toHexString(byte[] array) {
	   return toHexString(array, 0, array.length);
	}
			
	public static String toHexString(byte[] array, int offset, int length) {
	   StringBuilder sb = new StringBuilder();
	   for (int i = offset; i < offset + length; i++) {
		   sb.append(String.format("%02x", array[i] & 0xff));
	   }
	   return sb.toString();
	}
	
	public static String toHexString(UUID uuid) {
		return uuid.toString().replace("-", "");
	}
	
	public static String toHexStringSegment(byte[] array, int offset, int range) {
	   StringBuilder sb = new StringBuilder();
	   int start = offset - range;
	   start = start < 0? 0 : start;
	   
	   int end = offset + range;
	   end = end > array.length? array.length: end;
	   
	   for (int i = start; i < end; i++) {
		   if (i == offset) {
			   sb.append('[');
		   }
		   sb.append(String.format("%02x", array[i] & 0xff));
		   if (i == offset) {
			   sb.append(']');
		   }
	   }
	   return sb.toString();
	}
	
	/**
	 * Gets the next power of 2 for v.
	 * Used from http://graphics.stanford.edu/~seander/bithacks.html#RoundUpPowerOf2.
	 * @param v
	 * @return the next highest power of 2 for v
	 */
	public static int nextPowerOf2(int v) {
		if (v == 0) return 1;
		v--;
		v |= v >>> 1;
		v |= v >>> 2;
		v |= v >>> 4;
		v |= v >>> 8;
		v |= v >>> 16;
		v++;
		return v;
	}
	
	private static final Pattern ZK_ROOT_PATTERN = Pattern.compile("^(/|(/[A-Za-z0-9]+)+)$");
	private static final Pattern ZK_PATH_PATTERN = Pattern.compile("^(/[A-Za-z0-9]+)+$");
	
	public static boolean isValidZKRoot(String path) {
		return ZK_ROOT_PATTERN.matcher(path).matches();
	}
	
	public static boolean isValidZKPath(String path) {
		return ZK_PATH_PATTERN.matcher(path).matches();
	}
	
	public static <Config extends Configuration> Config readConfig(Class<Config> configClass, String path) {
		return readConfig(configClass, Paths.get(path));
	}
	
	public static <Config extends Configuration> Config readConfig(Class<Config> configClass, Path path) {
		Constructor constructor = new Constructor();
		Yaml yaml = new Yaml(constructor);
		constructor.getPropertyUtils().setSkipMissingProperties(true);
		
		try (InputStream configStream = Files.newInputStream(path, StandardOpenOption.READ)) {
			Config config = yaml.loadAs(configStream, configClass);
			return config;
		} catch (Exception e) {
			throw new RuntimeException("Error reading configuration file", e);
		}
	}
	
	public static <T> Map<String, String> parseCommandLine(String processName, CommandLineConfiguration<T> configHandler, String[] args) {
		return parseCommandLine(processName, configHandler.getCommandLineOptions(), args, true);
	}
	
	public static Map<String, String> parseCommandLine(String processName, Iterable<Option> options, String[] args, boolean ignoreUnknownOptions) {
		Map<String, String> parsedCommandLine = new HashMap<>();
		Options cliOptions = new Options();
		for (Option option : options) {
			cliOptions.addOption(option);
			parsedCommandLine.put(option.getOpt(), null);
		}
		CommandLineParser parser = new TolerantParser(ignoreUnknownOptions);
		try {
			CommandLine commandLine = parser.parse(cliOptions, args);
			for (Entry<String, String> entry : new HashSet<>(parsedCommandLine.entrySet())) {
				parsedCommandLine.put(entry.getKey(), commandLine.getOptionValue(entry.getKey()).trim());
			}
		} catch (ParseException eParse) {
			HelpFormatter helpFormatter = new HelpFormatter();
			helpFormatter.printHelp(String.format("%s [options]", processName), cliOptions);
			System.exit(1);
		}
		return parsedCommandLine;
	}
	
	@SafeVarargs
	public static <T> Iterable<T> newIterable(Iterable<T> existingIterable, T... entries) {
		List<T> entriesList = Arrays.asList(entries);
		for (T existingEntry : existingIterable) {
			entriesList.add(existingEntry);
		}
		return entriesList;
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
	
	public static <TConfig extends Configuration> void generateConfigFile(TConfig defaultConfig, Path configPath) throws IOException {
		Yaml yaml = new Yaml();
		File configFile = configPath.toFile();
		if (configFile.isDirectory()) {
			throw new IllegalArgumentException(String.format("The config file path was a directory '%s'", configPath.toString()));
		} else if (configFile.exists()) {
			throw new IllegalArgumentException(String.format("The config file already exists '%s'", configPath.toString()));
		}
		try (BufferedWriter writer = Files.newBufferedWriter(configPath, Charsets.UTF_8, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE)) {
			yaml.dump(defaultConfig, writer);
		}
	}
	
	public static <TRunnable extends Runnable> StatefulRunnable<TRunnable> asStateful(TRunnable runnable) {
		return new StatefulRunnable<TRunnable>(runnable);
	}
		
	public static long getCurrentMetricBucketId() {
		return System.currentTimeMillis() / Constants.METRIC_TICK;
	}
	
	public static long getMetricBucketStartTime(long metricBucketId) {
		return metricBucketId * Constants.METRIC_TICK;
	}
	
	public static long getMetricBucketEndTime(long metricBucketId) {
		return (metricBucketId + 1) * Constants.METRIC_TICK;
	}
	
}
