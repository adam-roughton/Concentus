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

import com.adamroughton.consentus.Util;
import com.adamroughton.consentus.messaging.events.EventType;

public final class SocketSettings {

	private final String[] _inprocNamesToBindTo;
	private final int[] _portsToBindTo;
	private final long _hwm;
	private final byte[][] _subscriptions;
	
	public static SocketSettings create() {
		return new SocketSettings(new String[0], new int[0], -1, new byte[0][]);
	}
	
	private SocketSettings(
			final String[] inprocNamesToBindTo,
			final int[] portsToBindTo, 
			final long hwm, 
			final byte[][] subscriptions) {
		_inprocNamesToBindTo = inprocNamesToBindTo;
		_portsToBindTo = portsToBindTo;
		_hwm = hwm;
		_subscriptions = subscriptions;
	}
	
	public SocketSettings bindToPort(final int port) {
		Util.assertPortValid(port);
		int[] portsToBindTo = new int[_portsToBindTo.length + 1];
		System.arraycopy(_portsToBindTo, 0, portsToBindTo, 0, _portsToBindTo.length);
		portsToBindTo[_portsToBindTo.length] = port;
		return new SocketSettings(_inprocNamesToBindTo, portsToBindTo, _hwm, _subscriptions);
	}
	
	/**
	 * Binds the socket to {@code inproc://(name)}
	 * @param name the unique name to bind to
	 * @return
	 */
	public SocketSettings bindToInprocName(final String name) {
		Objects.requireNonNull(name);
		if (name.length() > 256) 
			throw new IllegalArgumentException(
				String.format("The name must be 256 characters or less (%s).", name));
		String[] inprocNamesToBindTo = new String[_inprocNamesToBindTo.length + 1];
		System.arraycopy(_inprocNamesToBindTo, 0, inprocNamesToBindTo, 0, _inprocNamesToBindTo.length);
		inprocNamesToBindTo[_inprocNamesToBindTo.length] = name;
		return new SocketSettings(inprocNamesToBindTo, _portsToBindTo, _hwm, _subscriptions);
	}
	
	public SocketSettings setHWM(final int hwm) {
		if (hwm < 0)
			throw new IllegalArgumentException("The HWM must be 0 or greater.");
		return new SocketSettings(_inprocNamesToBindTo, _portsToBindTo, hwm, _subscriptions);
	}
	
	public SocketSettings subscribeTo(final EventType eventType) {
		return subscribeTo(eventType.getId());
	}
	
	public SocketSettings subscribeTo(final int id) {
		byte[] subId = new byte[4];
		MessageBytesUtil.writeInt(subId, 0, id);
		return subscribeTo(subId);
	}
	
	public SocketSettings subscribeToAll() {
		return subscribeTo(new byte[0]);
	}
	
	public SocketSettings subscribeTo(final byte[] subscription) {
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
		return new SocketSettings(_inprocNamesToBindTo, _portsToBindTo, _hwm, subscriptions);
	}
	
	public String[] getInprocNamesToBindTo() {
		return Arrays.copyOf(_inprocNamesToBindTo, _inprocNamesToBindTo.length);
	}
	
	public int[] getPortsToBindTo() {
		return Arrays.copyOf(_portsToBindTo, _portsToBindTo.length);
	}
	
	public boolean isBound() {
		return _portsToBindTo.length != 0;
	}
	
	public long getHWM() {
		return _hwm;
	}
	
	public byte[][] getSubscriptions() {
		byte[][] subscriptions = new byte[_subscriptions.length][];
		for (int i = 0; i < subscriptions.length; i++) {
			byte[] sub = _subscriptions[i];
			subscriptions[i] = Arrays.copyOf(sub, sub.length);
		}
		return subscriptions;
	}
	
	public void configureSocket(ZMQ.Socket socket) {
		long hwm = this.getHWM();
		if (hwm != -1) {
			socket.setHWM(hwm);
		}
		for (byte[] subscription : _subscriptions) {
			socket.subscribe(subscription);
		}
		for (String inprocName : _inprocNamesToBindTo) {
			socket.bind(getInprocAddress(inprocName));
		}
		for (int port : _portsToBindTo) {
			socket.bind("tcp://*:" + port);
		}
	}
	
	public static String getInprocAddress(String inprocName) {
		return String.format("inproc://%s", inprocName);
	}
	
}
