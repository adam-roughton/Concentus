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
package com.adamroughton.concentus.data.model;

public final class ClientId {
	
	private static final long CLIENT_ID_NAMESPACE_MASK = (0xFFFFL << 48);
	
	private final int _namespaceId;
	private final long _clientId;

	/**
	 * Each client ID is 64 bits with 16 bits reserved for the
	 * namespaceID, and 48 bits registered for the clientId.
	 * @param namespaceId the ID for which this client ID is unique. Only the first 16 bits
	 * will be used.
	 * @param clientId the unique client ID in the namespace. Only the first 48 bits
	 * will be used.
	 */
	public ClientId(final int namespaceId, final long clientId) {
		_namespaceId = namespaceId;
		_clientId = clientId;
	}
	
	public int getNamespaceId() {
		return _namespaceId;
	}

	public long getClientId() {
		return _clientId;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + (int) (_clientId ^ (_clientId >>> 32));
		result = prime * result + _namespaceId;
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		ClientId other = (ClientId) obj;
		if (_clientId != other._clientId)
			return false;
		if (_namespaceId != other._namespaceId)
			return false;
		return true;
	}
	
	public long toBits() {
		return toBits(this);
	}
	
	public static ClientId fromBits(long clientIdBits) {
		int namespaceId = (int)((clientIdBits & CLIENT_ID_NAMESPACE_MASK) >>> 48);
		long clientId = clientIdBits & (~CLIENT_ID_NAMESPACE_MASK);
		return new ClientId(namespaceId, clientId);
	}
	
	public static long toBits(ClientId clientId) {
		long clientIdBits = clientId.getNamespaceId();
		clientIdBits <<= 48;
		clientIdBits &= CLIENT_ID_NAMESPACE_MASK;
		clientIdBits |= (clientId.getClientId() & (~CLIENT_ID_NAMESPACE_MASK));
		return clientIdBits;
	}
	
}
