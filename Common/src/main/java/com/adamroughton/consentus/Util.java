package com.adamroughton.consentus;

public class Util {

	public static void assertPortValid(int port) {
		if (port < 1024 || port > 65535)
			throw new RuntimeException(String.format("Bad port number: %d", port));
	}
	
}
