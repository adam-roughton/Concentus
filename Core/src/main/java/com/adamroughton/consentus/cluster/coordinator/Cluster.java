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
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import com.adamroughton.consentus.cluster.worker.ClusterStateValue;

public interface Cluster {
	
	List<String> getAssignmentRequestServiceTypes();
	
	List<AssignmentRequest> getAssignmentRequests(String serviceType);
	
	void setAssignment(String serviceType, UUID serviceId, byte[] assignment);
	
	void setState(ClusterStateValue state);
	
	/**
	 * Takes a snapshot of all of the participating nodes, allowing
	 * them to be tracked for operations such as {@link Cluster#waitForReady(ParticipatingNodes))}.
	 * @return an entity that can be used to track nodes.
	 */
	ParticipatingNodes getNodeSnapshot();
	
	boolean waitForReady(ParticipatingNodes participatingNodes) throws InterruptedException;
	
	boolean waitForReady(ParticipatingNodes participatingNodes, long time, TimeUnit unit) throws InterruptedException;
	
	UUID getMyId();
	
	public static class AssignmentRequest {
		private final String _serviceId;
		private final byte[] _requestBytes;
		
		public AssignmentRequest(String serviceId, byte[] requestBytes) {
			_serviceId = Objects.requireNonNull(serviceId);
			_requestBytes = Objects.requireNonNull(requestBytes);
		}
		
		public String getServiceId() {
			return _serviceId;
		}
		
		public byte[] getRequestBytes() {
			byte[] copy = new byte[_requestBytes.length];
			System.arraycopy(_requestBytes, 0, copy, 0, copy.length);
			return copy;
		}
	}
	
	
	
}
