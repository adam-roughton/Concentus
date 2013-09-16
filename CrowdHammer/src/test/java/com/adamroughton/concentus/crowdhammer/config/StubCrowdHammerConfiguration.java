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
package com.adamroughton.concentus.crowdhammer.config;

import java.util.Map;

import com.adamroughton.concentus.config.ServiceConfig;
import com.adamroughton.concentus.config.ZooKeeper;
import com.adamroughton.concentus.configuration.StubConfiguration;

public class StubCrowdHammerConfiguration extends CrowdHammerConfiguration {

	private final StubConfiguration _stubConfiguration = new StubConfiguration();
	private final CrowdHammer _crowdHammer;
	
	public StubCrowdHammerConfiguration(int testDurationSeconds) {
		_crowdHammer = new CrowdHammer();
		_crowdHammer.setTestRunDurationInSeconds(testDurationSeconds);
	}
	
	@Override
	public CrowdHammer getCrowdHammer() {
		return _crowdHammer;
	}

	@Override
	public void setCrowdHammer(CrowdHammer crowdHammer) {
	}

	@Override
	public String getWorkingDir() {
		return _stubConfiguration.getWorkingDir();
	}

	@Override
	public void setWorkingDir(String workingDir) {
		_stubConfiguration.setWorkingDir(workingDir);
	}

	@Override
	public ZooKeeper getZooKeeper() {
		return _stubConfiguration.getZooKeeper();
	}

	@Override
	public void setZooKeeper(ZooKeeper zooKeeper) {
		_stubConfiguration.setZooKeeper(zooKeeper);
	}

	@Override
	public Map<String, ServiceConfig> getServices() {
		return _stubConfiguration.getServices();
	}

	@Override
	public void setServices(Map<String, ServiceConfig> services) {
		_stubConfiguration.setServices(services);
	}
}
