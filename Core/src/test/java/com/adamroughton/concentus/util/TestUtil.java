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

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.junit.*;

import com.adamroughton.concentus.config.Configuration;
import com.adamroughton.concentus.config.ServiceConfig;
import com.adamroughton.concentus.config.ZooKeeper;
import com.adamroughton.concentus.util.Util;

import static org.junit.Assert.*;

public class TestUtil {

	@Test
	public void nextPowerOf2() {
		long expected = 1024;
		assertEquals(expected, Util.nextPowerOf2(567));
	}
	
	@Test
	public void nextPowerOf2_Boundary() {
		long expected = 512;
		assertEquals(expected, Util.nextPowerOf2(512));
	}
	
	@Test
	public void nextPowerOf2_Zero() {
		long expected = 1;
		assertEquals(expected, Util.nextPowerOf2(0));
	}
	
	@Test
	public void nextPowerOf2_JavaNegative() {
		int expected = 0;
		assertEquals(expected, Util.nextPowerOf2(0xA0000000));
	}
	
	@Test
	public void nextPowerOf2_PenultimateBitSet() {
		int expected = 0x80000000;
		assertEquals(expected, Util.nextPowerOf2(0x40000001));
	}
	
	@Test
	public void testWriteConfig() throws Exception {
		Path tmpDirPath = Files.createTempDirectory("Consentus");
		try {
			Path configPath = tmpDirPath.resolve("testConfig.yaml");
			
			// create config
			Configuration config = new Configuration();
			Map<String, ServiceConfig> services = new HashMap<>();
			int port = 23000;
			for (int i = 0; i < 5; i++) {
				ServiceConfig service = new ServiceConfig();
				service.setName("TestService" + i);
				Map<String, Integer> ports = new HashMap<>();
				for (int j = 0; j < i + 2; j++) {
					ports.put("input" + j, port++);
				}
				service.setPorts(ports);
				services.put("testService" + i, service);
			}
			config.setWorkingDir("/tmp/consentus1234");
			config.setServices(services);
			ZooKeeper zooKeeper = new ZooKeeper();
			zooKeeper.setAppRoot("/Consentus");
			config.setZooKeeper(zooKeeper);
			
			Util.generateYamlFile(config, configPath);
			
			Configuration configWritten = Util.readYamlFile(Configuration.class, configPath);
			assertEquals(config, configWritten);
		} finally {
			FileUtils.deleteQuietly(tmpDirPath.toFile());
		}
	}
	
	@Test
	public void binarySearchHighestMatch_StartIndexGreaterThanEnd() {
		assertEquals(-1, Util.binarySearchHighestMatch(new long[] {1, 4, 6, 7, 9}, 1, 5, 0));
	}
	
	@Test
	public void binarySearchHighestMatch_MatchesFirstElement() {
		assertEquals(1, Util.binarySearchHighestMatch(new long[] {1, 2, 4, 5, 7, 9, 10}, 1, 0, 6));
	}
	
	@Test
	public void binarySearchHighestMatch_MatchesLastElement() {
		assertEquals(10, Util.binarySearchHighestMatch(new long[] {1, 2, 4, 5, 7, 9, 10}, 10, 0, 6));
	}
	
	@Test
	public void binarySearchHighestMatch_MatchesRightOfMidOnEven() {
		assertEquals(8, Util.binarySearchHighestMatch(new long[]{1, 2, 5, 8, 9, 11}, 8, 0, 5));
	}
	
	@Test
	public void binarySearchHighestMatch_MatchesMidOnEven() {
		assertEquals(5, Util.binarySearchHighestMatch(new long[]{1, 2, 5, 8, 9, 11}, 5, 0, 5));
	}
	
	@Test
	public void binarySearchHighestMatch_MatchesOnlyElement() {
		assertEquals(1, Util.binarySearchHighestMatch(new long[]{1}, 1, 0, 0));
	}
	
	@Test
	public void binarySearchHighestMatch_NonDirectMatchEven() {
		assertEquals(5, Util.binarySearchHighestMatch(new long[]{1, 2, 5, 8, 9, 11}, 6, 0, 5));
	}
	
	@Test
	public void binarySearchHighestMatch_NonDirectMatchOdd() {
		assertEquals(5, Util.binarySearchHighestMatch(new long[]{1, 2, 5, 8, 9, 11, 46}, 6, 0, 6));
	}
	
	@Test
	public void binarySearchHighestMatch_NonDirectMatchManyPossible() {
		assertEquals(11, Util.binarySearchHighestMatch(new long[]{6, 7, 8, 9, 10, 11}, 12, 0, 5));
	}
	
	@Test
	public void binarySearchHighestMatch_DoesNotMatchEven() {
		assertEquals(-1, Util.binarySearchHighestMatch(new long[]{5, 7, 9, 11, 15, 63}, 4, 0, 5));
	}
	
	@Test
	public void binarySearchHighestMatch_DoesNotMatchOdd() {
		assertEquals(-1, Util.binarySearchHighestMatch(new long[]{5, 7, 9, 11, 15, 63, 121}, 4, 0, 6));
	}	
	
}

