package com.adamroughton.consentus;

import com.adamroughton.consentus.messaging.events.EventType;
import com.esotericsoftware.kryo.Kryo;

public class Util {

	public static void assertPortValid(int port) {
		if (port < 1024 || port > 65535)
			throw new RuntimeException(String.format("Bad port number: %d", port));
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
}
