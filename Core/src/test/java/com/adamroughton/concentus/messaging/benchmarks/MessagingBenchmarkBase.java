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
package com.adamroughton.concentus.messaging.benchmarks;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.zeromq.ZMQ;

import com.google.caliper.Benchmark;

public abstract class MessagingBenchmarkBase extends Benchmark {
	
	private ExecutorService _executor;
	private AtomicBoolean _interactingPartyRunFlag;
	private Future<?> _interactingPartyTask;
	private AtomicReference<Thread> _interactingPartyTaskThread;
	
	private ZMQ.Context _zmqContext;
	
	@Override
	protected final void setUp() throws Exception {
		super.setUp();
		_interactingPartyRunFlag = new AtomicBoolean(true);
		_executor = Executors.newCachedThreadPool();
		_zmqContext = ZMQ.context(1);
		setUp(_zmqContext);
		_interactingPartyTaskThread = new AtomicReference<Thread>(null);
		final AtomicReference<Thread> ref = _interactingPartyTaskThread;
		_interactingPartyTask = _executor.submit(new Runnable() {

			@Override
			public void run() {
				ref.set(Thread.currentThread());
				createInteractingParty(_zmqContext, _interactingPartyRunFlag).run();
			}
			
		});
		final Thread mainThread = Thread.currentThread();
		_executor.submit(new Runnable() {

			@Override
			public void run() {
				try {
					_interactingPartyTask.get();
				} catch (Exception e) {
					e.printStackTrace();
					mainThread.interrupt();
				}
			}
			
		});
	}
	
	protected void setUp(ZMQ.Context context) throws Exception {
	}
	
	protected abstract Runnable createInteractingParty(ZMQ.Context context, AtomicBoolean runFlag);

	@Override
	protected void tearDown() throws Exception {
		super.tearDown();
		_interactingPartyRunFlag.set(false);
		Thread interactingPartyTaskThread = _interactingPartyTaskThread.get();
		if (interactingPartyTaskThread != null) {
			interactingPartyTaskThread.interrupt();
		}
		
		if (_interactingPartyTask != null) {
			_interactingPartyTask.get();
		}
		if (_zmqContext != null) {
			_zmqContext.term();
		}
	}
	
}