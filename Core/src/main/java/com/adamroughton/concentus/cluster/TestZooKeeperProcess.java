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
package com.adamroughton.concentus.cluster;

import com.adamroughton.concentus.Util;
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
