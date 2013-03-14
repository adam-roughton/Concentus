package com.adamroughton.consentus.cluster;

import com.adamroughton.consentus.Util;
import com.netflix.curator.test.TestingServer;

/**
 * Starts a Curator Test Server instance and prints out the 
 * address.
 * 
 * @author Adam Roughton
 *
 */
public class TestZooKeeperProcess {

	public static void main(String[] args) throws Exception {
		if (args.length < 1) {
			System.out.println("Usage: [port]");
		}
		int port = Util.getPort(args[0]);
		
		try (TestingServer testServer = new TestingServer(port)) {
			System.out.println(testServer.getConnectString());
			
			// Wait to be killed by user
			Object monitor = new Object();
			synchronized(monitor) {
				monitor.wait();
			}
		}
	}
	
}
