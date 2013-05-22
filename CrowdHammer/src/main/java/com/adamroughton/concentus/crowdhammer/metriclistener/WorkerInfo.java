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
package com.adamroughton.concentus.crowdhammer.metriclistener;

public class WorkerInfo {
	
	private long _workerId;
	private int _clientCount;
	
	public WorkerInfo() {
	}
	
	public WorkerInfo(long workerId, int simClientCount) {
		_workerId = workerId;
		_clientCount = simClientCount;
	}
	
	public long getWorkerId() {
		return _workerId;
	}
	public void setWorkerId(long workerId) {
		_workerId = workerId;
	}
	public int getSimClientCount() {
		return _clientCount;
	}
	public void setSimClientCount(int simClientCount) {
		_clientCount = simClientCount;
	}

}
