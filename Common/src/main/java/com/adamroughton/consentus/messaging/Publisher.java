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

import java.util.Objects;

import org.zeromq.ZMQ;

import com.esotericsoftware.minlog.Log;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.LifecycleAware;

public class Publisher implements EventHandler<byte[]>, LifecycleAware {

	private final ZMQ.Context _zmqContext;
	private final SocketSettings _socketSetting;
	private final EventSender _eventSender;
	private final MessagePartBufferPolicy _msgPartPolicy;

	private ZMQ.Socket _pub;
	
	public Publisher(final ZMQ.Context zmqContext,
			final SocketSettings socketSetting,
			final EventProcessingHeader processingHeader) {
		if (socketSetting.getSocketType() != ZMQ.PUB && socketSetting.getSocketType() != ZMQ.XPUB) {
			throw new IllegalArgumentException(String.format("The socket type was %d, not PUB or XPUB", 
					socketSetting.getSocketType()));
		}
		_zmqContext = Objects.requireNonNull(zmqContext);
		_socketSetting = Objects.requireNonNull(socketSetting);
		_eventSender = new EventSender(processingHeader, false);
		_msgPartPolicy = _socketSetting.getMessagePartPolicy();
	}
	
	@Override
	public void onStart() {
		_pub = _zmqContext.socket(_socketSetting.getSocketType());
		_pub.setHWM(_socketSetting.getHWM());
		for (int bindPort : _socketSetting.getPortsToBindTo()) {
			_pub.bind(String.format("tcp://*:%d", bindPort));
		}
		for (String connString : _socketSetting.getConnectionStrings()) {
			_pub.connect(connString);
		}		
	}

	@Override
	public void onShutdown() {
		if (_pub != null) {
			try {
				_pub.close();
			} catch (Exception eClose) {
				Log.warn("Exception thrown when closing ZMQ socket.", eClose);
			}
			_pub = null;
		}
	}

	@Override
	public void onEvent(byte[] event, long sequence, boolean endOfBatch)
			throws Exception {
		_eventSender.send(_pub, event, _msgPartPolicy);
	}

}


