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
package com.adamroughton.consentus.messaging;

import java.util.Arrays;
import java.util.Objects;

import org.zeromq.ZMQ;

import com.adamroughton.consentus.messaging.events.EventType;

public final class SubSocketSettings {
	
	private final SocketSettings _socketSettings;
	private final byte[][] _subscriptions;
	
	public static SubSocketSettings create(final SocketSettings socketSettings) {
		Objects.requireNonNull(socketSettings);
		if (!isSubSocket(socketSettings.getSocketType())) {
			throw new IllegalArgumentException("SubSocketSettings can only be applied to subscription based sockets.");
		}
		return new SubSocketSettings(socketSettings, new byte[0][0]);
	}
	
	private SubSocketSettings(final SocketSettings socketSettings, 
			final byte[][] subscriptions) {
		_socketSettings = socketSettings;
		_subscriptions = subscriptions;
	}
	
	public SubSocketSettings subscribeTo(final EventType eventType) {
		return subscribeTo(eventType.getId());
	}
	
	public SubSocketSettings subscribeTo(final int id) {
		byte[] subId = new byte[4];
		MessageBytesUtil.writeInt(subId, 0, id);
		return subscribeTo(subId);
	}
	
	public SubSocketSettings subscribeToAll() {
		return subscribeTo(new byte[0]);
	}
	
	public SubSocketSettings subscribeTo(final byte[] subscription) {
		Objects.requireNonNull(subscription);
		byte[][] subscriptions = new byte[_subscriptions.length + 1][];
		for (int i = 0; i < subscriptions.length; i++) {
			byte[] sub;
			if (i == 0) {
				sub = subscription;
			} else {
				sub = _subscriptions[i - 1];
			}
			subscriptions[i] = Arrays.copyOf(sub, sub.length);
		}
		return new SubSocketSettings(_socketSettings, subscriptions);
	}
	
	public byte[][] getSubscriptions() {
		byte[][] subscriptions = new byte[_subscriptions.length][];
		for (int i = 0; i < subscriptions.length; i++) {
			byte[] sub = _subscriptions[i];
			subscriptions[i] = Arrays.copyOf(sub, sub.length);
		}
		return subscriptions;
	}
	
	public SocketSettings getSocketSettings() {
		return _socketSettings;
	}
	
	public static boolean isSubSocket(int socketType) {
		return socketType == ZMQ.SUB || socketType == ZMQ.XSUB;
	}
	
	public static SubSocketSettings wrapImplicit(final SocketSettings socketSetting) {
		if (!isSubSocket(socketSetting.getSocketType()))
			throw new IllegalArgumentException("The socket type must be a sub type to " +
					"wrap it with a SubSocketSettings wrapper.");
		return SubSocketSettings.create(socketSetting).subscribeToAll();
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result
				+ ((_socketSettings == null) ? 0 : _socketSettings.hashCode());
		result = prime * result + Arrays.hashCode(_subscriptions);
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
		SubSocketSettings other = (SubSocketSettings) obj;
		if (_socketSettings == null) {
			if (other._socketSettings != null)
				return false;
		} else if (!_socketSettings.equals(other._socketSettings))
			return false;
		if (!Arrays.deepEquals(_subscriptions, other._subscriptions))
			return false;
		return true;
	}
}
