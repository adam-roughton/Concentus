package com.adamroughton.consentus;

import com.adamroughton.consentus.messaging.MessageBytesUtil;
import com.adamroughton.consentus.messaging.events.EventType;
import com.esotericsoftware.kryo.Kryo;

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

}
