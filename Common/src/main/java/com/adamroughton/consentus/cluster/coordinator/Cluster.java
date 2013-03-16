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
package com.adamroughton.consentus.cluster.coordinator;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

public interface Cluster {
	
	List<byte[]> getAssignmentRequests(String serviceType);
	
	void setAssignment(String serviceType, UUID serviceId, byte[] assignment);
	
	void setState(int state);
	
	List<UUID> getWaitingServiceIDs();
	
	void waitForReady() throws InterruptedException;
	
	void waitForReady(long time, TimeUnit unit) throws InterruptedException;
	
	UUID getMyId();
	
}
