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
package com.adamroughton.concentus.util;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.regex.Pattern;

import org.apache.commons.io.Charsets;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;

import com.adamroughton.concentus.Clock;
import com.adamroughton.concentus.config.Configuration;
import com.adamroughton.concentus.data.BytesUtil;
import com.adamroughton.concentus.data.DataType;
import com.esotericsoftware.kryo.Kryo;
import com.lmax.disruptor.EventFactory;

public class Util {
	
	public static void assertPortValid(int port) {
		if (port == -1) return; // we reserve -1 to signal random port
		if (port < 1024 || port > 65535)
			throw new RuntimeException(String.format("Bad port number: %d", port));
	}
	
	public static int getPort(final String portString) {
		int port = Integer.parseInt(portString);
		Util.assertPortValid(port);
		return port;
	}
	
	public static Kryo newKryoInstance() {
		Kryo kryo = new Kryo();
		initialiseKryo(kryo);
		return kryo;
	}
	
	public static void initialiseKryo(Kryo kryo) {
		for (DataType dataType : DataType.values()) {
			dataType.register(kryo);
		}
	}
	
	public static byte[] getSubscriptionBytes(DataType eventType) {
		byte[] subId = new byte[4];
		BytesUtil.writeInt(subId, 0, eventType.getId());
		return subId;
	}
	
	public static byte[] intToBytes(int value) {
		byte[] intBytes = new byte[4];
		BytesUtil.writeInt(intBytes, 0, value);
		return intBytes;
	}
	
	public static byte[] longToBytes(long value) {
		byte[] longBytes = new byte[8];
		BytesUtil.writeLong(longBytes, 0, value);
		return longBytes;
	}
	
	public static byte[] floatToBytes(float value) {
		byte[] floatBytes = new byte[4];
		BytesUtil.writeFloat(floatBytes, 0, value);
		return floatBytes;
	}
	
	public static byte[] doubleToBytes(double value) {
		byte[] doubleBytes = new byte[8];
		BytesUtil.writeDouble(doubleBytes, 0, value);
		return doubleBytes;
	}
	
	public static int bytesToInt(byte[] bytes) {
		if (bytes.length != 4)
			throw new IllegalArgumentException("Expected 4 bytes for int");
		return BytesUtil.readInt(bytes, 0);
	}
	
	public static long bytesToLong(byte[] bytes) {
		if (bytes.length != 8)
			throw new IllegalArgumentException("Expected 8 bytes for long");
		return BytesUtil.readLong(bytes, 0);
	}
	
	public static float bytesToFloat(byte[] bytes) {
		if (bytes.length != 4)
			throw new IllegalArgumentException("Expected 4 bytes for float");
		return BytesUtil.readFloat(bytes, 0);
	}
	
	public static double bytesToDouble(byte[] bytes) {
		if (bytes.length != 8)
			throw new IllegalArgumentException("Expected 8 bytes for double");
		return BytesUtil.readDouble(bytes, 0);
	}
	
	public static void writeSubscriptionBytes(DataType eventType, byte[] buffer, int offset) {
		BytesUtil.writeInt(buffer, offset, eventType.getId());
	}
	
	public static EventFactory<byte[]> byteArrayEventFactory(final int msgBufferSize) {
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
	
	@SafeVarargs
	public static <T> Iterable<T> newIterable(Iterable<T> existingIterable, T... entries) {
		List<T> entriesList = Arrays.asList(entries);
		for (T existingEntry : existingIterable) {
			entriesList.add(existingEntry);
		}
		return entriesList;
	}
	
	public static long millisUntil(long deadline, Clock clock) {
		long remainingTime = deadline - clock.currentMillis();
		if (remainingTime < 0)
			remainingTime = 0;
		return remainingTime;
	}
	
	public static long nanosUntil(long deadline, Clock clock) {
		long remainingTime = deadline - clock.nanoTime();
		if (remainingTime < 0)
			remainingTime = 0;
		return remainingTime;
	}
	
	public static long binarySearchHighestMatch(long[] orderedValues, long key, int startIndex, int endIndex) {
		if (startIndex > endIndex) {
			return -1;
		} 
		int midIndex = startIndex + (endIndex - startIndex) / 2;
		long midValue = orderedValues[midIndex];
		if (midValue == key) {
			return key;
		} else if (midValue < key) {
			long nextHighestMatch = binarySearchHighestMatch(orderedValues, key, midIndex + 1, endIndex);
			if (nextHighestMatch == -1) {
				return midValue;
			} else {
				return nextHighestMatch;
			}
		} else {
			return binarySearchHighestMatch(orderedValues, key, startIndex, midIndex - 1);
		}
	}
	
	public static String statsToString(String name, RunningStats stats) {
		return String.format("%s: %d samples, %f mean, %f stdDev, %f max, %f min",
				name,
				stats.getCount(), 
				stats.getMean(), 
				stats.getStandardDeviation(), 
				stats.getMax(), 
				stats.getMin());
	}
	
	public static double getPercentage(long numerator, long denominator) {
		return ((double) numerator / (double) denominator) * 100;
	}
	
	@SuppressWarnings("unchecked")
	public static <TCast> TCast newInstance(String instanceClassName, Class<TCast> castType) {
		try {
			Class<?> instanceClass = Class.forName(instanceClassName);
			Object object = instanceClass.newInstance();
			
			if (!castType.isAssignableFrom(instanceClass)) {
				throw new RuntimeException(String.format("The class %s is not of type %s", 
						instanceClassName,
						castType.getName()));
			}
			return (TCast) object;
		} catch (ClassNotFoundException eNotFound){
			throw new RuntimeException(String.format("Could not find the instance class '%1$s'.", instanceClassName), eNotFound);
		} catch (InstantiationException | IllegalAccessException | SecurityException e) {
			throw new RuntimeException(String.format("Could not instantiate the instance class %1$s.", instanceClassName), e);
		}
	}
	
}
