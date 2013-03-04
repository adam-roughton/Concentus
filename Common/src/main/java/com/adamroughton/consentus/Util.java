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
package com.adamroughton.consentus;

import org.zeromq.ZMQ;

import com.adamroughton.consentus.messaging.MessageBytesUtil;
import com.adamroughton.consentus.messaging.SocketSettings;
import com.adamroughton.consentus.messaging.SubSocketSettings;
import com.adamroughton.consentus.messaging.events.EventType;
import com.esotericsoftware.kryo.Kryo;
import com.lmax.disruptor.EventFactory;

public class Util {
	
	public static void assertPortValid(int port) {
		if (port < 1024 || port > 65535)
			throw new RuntimeException(String.format("Bad port number: %d", port));
	}
	
	public static int getPort(final String portString) {
		int port = Integer.parseInt(portString);
		Util.assertPortValid(port);
		return port;
	}
	
	public static void initialiseKryo(Kryo kryo) {
		for (EventType eventType : EventType.values()) {
			kryo.register(eventType.getEventClass(), eventType.getId());
		}
	}
	
	public static Kryo createKryoInstance() {
		Kryo kryo = new Kryo();
		initialiseKryo(kryo);
		return kryo;
	}
	
	public static byte[] getSubscriptionBytes(EventType eventType) {
		byte[] subId = new byte[4];
		MessageBytesUtil.writeInt(subId, 0, eventType.getId());
		return subId;
	}
	
	public static EventFactory<byte[]> msgBufferFactory(final int msgBufferSize) {
		return new EventFactory<byte[]>() {
			public byte[] newInstance() {
				return new byte[msgBufferSize];
			}
		};
	}
	
	public static String toHexString(byte[] array) {
	   return toHexString(array, 0, array.length);
	}
			
	public static String toHexString(byte[] array, int offset, int length) {
	   StringBuilder sb = new StringBuilder();
	   for (int i = offset; i < offset + length; i++) {
		   sb.append(String.format("%02x", array[i] & 0xff));
	   }
	   return sb.toString();
	}
	
	public static String toHexStringSegment(byte[] array, int offset, int range) {
	   StringBuilder sb = new StringBuilder();
	   int start = offset - range;
	   start = start < 0? 0 : start;
	   
	   int end = offset + range;
	   end = end > array.length? array.length: end;
	   
	   for (int i = start; i < end; i++) {
		   if (i == offset) {
			   sb.append('[');
		   }
		   sb.append(String.format("%02x", array[i] & 0xff));
		   if (i == offset) {
			   sb.append(']');
		   }
	   }
	   return sb.toString();
	}
	
	public static ZMQ.Socket createSocket(final ZMQ.Context context, 
			final SocketSettings socketSettings) throws Exception {
		ZMQ.Socket socket = context.socket(socketSettings.getSocketType());
		long hwm = socketSettings.getHWM();
		if (hwm != -1) {
			socket.setHWM(hwm);
		}
		for (int port : socketSettings.getPortsToBindTo()) {
			socket.bind("tcp://*:" + port);
		}
		for (String address : socketSettings.getConnectionStrings()) {
			socket.connect(address);
		}
		return socket;
	}
	
	public static ZMQ.Socket createSubSocket(final ZMQ.Context context, 
			final SubSocketSettings subSocketSettings) throws Exception {
		ZMQ.Socket socket = createSocket(context, subSocketSettings.getSocketSettings());
		for (byte[] subId : subSocketSettings.getSubscriptions()) {
			socket.subscribe(subId);
		}
		return socket;
	}
}
