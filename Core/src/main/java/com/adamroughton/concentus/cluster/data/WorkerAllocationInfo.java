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
package com.adamroughton.concentus.cluster.data;

import java.util.UUID;

import com.adamroughton.concentus.messaging.MessageBytesUtil;

public class WorkerAllocationInfo {
	
	private UUID _workerId;
	private int _clientAllocation;
	
	public WorkerAllocationInfo() {
	}
	
	public WorkerAllocationInfo(UUID workerId, int clientAllocation) {
		_workerId = workerId;
		_clientAllocation = clientAllocation;
	}
	
	public UUID getWorkerId() {
		return _workerId;
	}
	public void setWorkerId(UUID workerId) {
		_workerId = workerId;
	}
	public int getClientAllocation() {
		return _clientAllocation;
	}
	public void setClientAllocation(int clientAllocation) {
		_clientAllocation = clientAllocation;
	}
	
	public byte[] getBytes() {
		return toBytes(this);
	}
	
	public static byte[] toBytes(WorkerAllocationInfo allocationInfo) {
		byte[] bytes = new byte[20];
		int offset = 0;
		MessageBytesUtil.writeUUID(bytes, offset, allocationInfo.getWorkerId());
		offset += 16;
		MessageBytesUtil.writeInt(bytes, offset, allocationInfo.getClientAllocation());
		return bytes;
	}
	
	public static WorkerAllocationInfo fromBytes(byte[] bytes) {
		int offset = 0;
		assertCanRead(bytes, offset, 16);
		UUID workerId = MessageBytesUtil.readUUID(bytes, offset);
		offset += 16;
		
		assertCanRead(bytes, offset, 4);
		int clientAllocation = MessageBytesUtil.readInt(bytes, offset);
		
		return new WorkerAllocationInfo(workerId, clientAllocation);
	}
	
	private static void assertCanRead(byte[] array, int offset, int length) {
		if (array.length < offset + length) 
			throw new IllegalArgumentException(String.format("The array was not long enough (tried to " +
					"read length %d at offset %d on array with length %d", length, offset, array.length));
	}

}
