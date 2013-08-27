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

import com.adamroughton.concentus.cluster.data.TestRunInfo;

public interface ClusterListenerHandle extends ClusterWorkerHandle {
	
	TestRunInfo getCurrentRunInfo();
	
	void requestAssignment(String serviceType, byte[] requestBytes);
	
	byte[] getAssignment(String serviceType);
	
	void deleteAssignmentRequest(String serviceType);
	
	void signalReady();
	
}
