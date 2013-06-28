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
package com.adamroughton.concentus.cluster.worker;

import java.util.List;
import java.util.UUID;

import com.adamroughton.concentus.cluster.data.MetricPublisherInfo;
import com.adamroughton.concentus.cluster.data.TestRunInfo;
import com.netflix.curator.framework.api.CuratorWatcher;

public interface ClusterWorkerHandle {
	
	TestRunInfo getCurrentRunInfo();
	
	/**
	 * Registers the worker as a metric publisher with the generated {@link ClusterWorkerHandle#getMyId() worker ID} for the cluster.
	 */
	void registerAsMetricPublisher(String type, String pubAddress, String metaDataReqAddress);
	
	List<MetricPublisherInfo> getMetricPublishers();
	
	List<MetricPublisherInfo> getMetricPublishers(CuratorWatcher watcher);
	
	void registerService(String serviceType, String address);
	
	void unregisterService(String serviceType);
	
	String getServiceAtRandom(String serviceType);
	
	String[] getAllServices(String serviceType);
	
	void requestAssignment(String serviceType, byte[] requestBytes);
	
	byte[] getAssignment(String serviceType);
	
	void deleteAssignmentRequest(String serviceType);
	
	void signalReady();
	
	UUID getMyId();
	
}
