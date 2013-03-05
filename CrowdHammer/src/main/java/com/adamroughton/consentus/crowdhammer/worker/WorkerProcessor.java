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
package com.adamroughton.consentus.crowdhammer.worker;

import java.util.Objects;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import org.zeromq.ZMQ;

import com.lmax.disruptor.FatalExceptionHandler;

import uk.co.real_logic.intrinsics.StructuredArray;

import static com.adamroughton.consentus.Util.*;

public final class WorkerProcessor {

	private final ZMQ.Context _zmqContext;
	private final StructuredArray<Client> _clients;
	
	// we allocate to the next power of 2 to make the wrapping around operation faster
	private final int _maxClients;
	private final String[] _clientHandlerConnStrings;
	private final FatalExceptionHandler _exHandler;
	
	private final Executor _executor = Executors.newCachedThreadPool();
	
	public WorkerProcessor(final int clientCount, 
			final ZMQ.Context zmqContext, 
			final String[] clientHandlerConnStrings,
			final FatalExceptionHandler exHandler) {
		_zmqContext = Objects.requireNonNull(zmqContext);
		_maxClients = clientCount;
		_clients = StructuredArray.newInstance(nextPowerOf2(clientCount), Client.class, new Class[] {ZMQ.Context.class}, zmqContext);
		_clientHandlerConnStrings = Objects.requireNonNull(clientHandlerConnStrings);
		if (_clientHandlerConnStrings.length == 0) 
			throw new IllegalArgumentException("At least one client handler connection string must be specified.");
		_exHandler = Objects.requireNonNull(exHandler);
	}
	
	public void init(final int clientCountForTest) {
		if (clientCountForTest > _maxClients)
			throw new IllegalArgumentException(
					String.format("The client count was too large: %d > %d", 
							clientCountForTest, 
							_maxClients));
		
		int nextConnString = 0;
		Client client;
		for (int i = 0; i < _clients.getLength(); i++) {
			client = _clients.get(i);
			if (i < clientCountForTest) {
				ZMQ.Socket clientSocket = client.getSocket();
				String connString = _clientHandlerConnStrings[nextConnString++ % _clientHandlerConnStrings.length];
				client.setClientHandlerConnString(connString);
				clientSocket.connect(connString);
				client.setIsActive(true);
				try {
					Thread.sleep(10);
				} catch (InterruptedException eInterrupt) {
				}
			} else {
				client.setIsActive(false);
			}
		}
	}
	
	public void runTest() {
		_executor.execute(new Runnable() {

			private volatile boolean isRunning = false;
			
			@Override
			public void run() {
				while (isRunning) {
					
				}
			}
			
		});
		
		
		// late metric
	
	}
	
	public void stopSendingInputEvents() {
		
	}
	
	public void teardown() {
		Client client;
		for (int i = 0; i < _clients.getLength(); i++) {
			client = _clients.get(i);
			if (client.isActive()) {
				String connString = _clients.get(i).getClientHandlerConnString();
				_clients.get(i).getSocket().disconnect(connString);
			}
		}
	}
	
}
