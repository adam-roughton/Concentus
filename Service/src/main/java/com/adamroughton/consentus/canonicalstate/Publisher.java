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
package com.adamroughton.consentus.canonicalstate;

import java.util.Objects;

import org.zeromq.ZMQ;
import org.zeromq.ZMQException;

import com.adamroughton.consentus.Config;
import com.adamroughton.consentus.Util;
import com.adamroughton.consentus.messaging.MessageBytesUtil;
import com.esotericsoftware.minlog.Log;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.LifecycleAware;

class Publisher implements EventHandler<byte[]>, LifecycleAware {

	private final ZMQ.Context _zmqContext;
	private final int _publishPort;

	private ZMQ.Socket _pub;
	
	public Publisher(ZMQ.Context zmqContext,
			Config conf) {
		_zmqContext = Objects.requireNonNull(zmqContext);
		
		_publishPort = Integer.parseInt(conf.getCanonicalStatePubPort());
		Util.assertPortValid(_publishPort);
	}
	
	@Override
	public void onStart() {
		_pub = _zmqContext.socket(ZMQ.PUB);
		_pub.setHWM(100);
		_pub.bind("tcp://*:" + _publishPort);
		Log.info(String.format("Publishing on port %d", _publishPort));
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
		try {
			// check if the error flag is raised
			if (!isValid(event)) {
				return;
			}			
			_pub.send(getSubId(event), ZMQ.SNDMORE);
			_pub.send(event, 1, 0);
		} catch (ZMQException eZmq) {
			if (eZmq.getErrorCode() != ZMQ.Error.ETERM.getCode()) {
				throw eZmq;
			}
		}
	}
	
	private static boolean isValid(byte[] event) {
		return !MessageBytesUtil.readFlagFromByte(event, 0, 0);
	}

	private byte[] getSubId(byte[] event) {
		byte[] subId = new byte[4];
		System.arraycopy(event, 1, subId, 0, 4);
		return subId;
	}
	
}


