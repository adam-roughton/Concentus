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
package com.adamroughton.consentus.config;

import java.util.Map;

public class Configuration {

	private String _workingDir;
	private ZooKeeper _zooKeeper;
	private Map<String, Service> _services;
	
	public String getWorkingDir() {		
		return _workingDir;
	}
	
	public void setWorkingDir(String workingDir) {
		_workingDir = workingDir;
	}
	
	public ZooKeeper getZooKeeper() {
		return _zooKeeper;
	}
	
	public void setZooKeeper(ZooKeeper zooKeeper) {
		_zooKeeper = zooKeeper;
	}
	
	public Map<String, Service> getServices() {
		return _services;
	}
	
	public void setServices(Map<String, Service> services) {
		_services = services;
	}
	
}
