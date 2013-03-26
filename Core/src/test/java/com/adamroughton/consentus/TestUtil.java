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

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.junit.*;

import com.adamroughton.consentus.Util;
import com.adamroughton.consentus.config.Configuration;
import com.adamroughton.consentus.config.Service;
import com.adamroughton.consentus.config.ZooKeeper;

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
			Map<String, Service> services = new HashMap<>();
			int port = 23000;
			for (int i = 0; i < 5; i++) {
				Service service = new Service();
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
			
			Util.generateConfigFile(config, configPath);
			
			Configuration configWritten = Util.readConfig(Configuration.class, configPath);
			assertEquals(config, configWritten);
		} finally {
			FileUtils.deleteQuietly(tmpDirPath.toFile());
		}
	}
}
