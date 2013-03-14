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
