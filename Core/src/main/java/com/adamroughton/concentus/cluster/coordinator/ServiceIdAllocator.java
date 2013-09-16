package com.adamroughton.concentus.cluster.coordinator;

public interface ServiceIdAllocator {

	int newMetricSource(String name, String serviceType);
	
}
