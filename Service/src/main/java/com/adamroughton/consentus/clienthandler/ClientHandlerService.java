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
package com.adamroughton.consentus.clienthandler;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.zeromq.ZMQ;

import com.adamroughton.consentus.Config;
import com.adamroughton.consentus.ConsentusProcessCallback;
import com.adamroughton.consentus.ConsentusService;
import com.adamroughton.consentus.messaging.EventListener;
import com.adamroughton.consentus.messaging.EventProcessingHeader;
import com.adamroughton.consentus.messaging.MessagePartBufferPolicy;
import com.adamroughton.consentus.messaging.Publisher;
import com.adamroughton.consentus.messaging.RouterSocketReactor;
import com.adamroughton.consentus.messaging.SocketSettings;
import com.adamroughton.consentus.messaging.SubSocketSettings;
import com.adamroughton.consentus.messaging.events.EventType;
import com.lmax.disruptor.MultiThreadedClaimStrategy;
import com.lmax.disruptor.SequenceBarrier;
import com.lmax.disruptor.SingleThreadedClaimStrategy;
import com.lmax.disruptor.YieldingWaitStrategy;
import com.lmax.disruptor.dsl.Disruptor;

import static com.adamroughton.consentus.Util.*;
import static com.adamroughton.consentus.Constants.*;

public class ClientHandlerService implements ConsentusService {
	
	private ExecutorService _executor;
	private Disruptor<byte[]> _recvDisruptor;
	private Disruptor<byte[]> _directSendDisruptor;
	private Disruptor<byte[]> _pubSendDisruptor;
	
	private EventListener _updateListener;
	private RouterSocketReactor _directRecvReactor;
	private ClientHandlerProcessor _processor;
	private Publisher _publisher;
	
	private ZMQ.Context _zmqContext;
	
	private int _clientHandlerId;
	
	@SuppressWarnings("unchecked")
	@Override
	public void start(final Config config, final ConsentusProcessCallback exHandler) {
		_executor = Executors.newCachedThreadPool();
		_zmqContext = ZMQ.context(1);
		
		_recvDisruptor = new Disruptor<>(msgBufferFactory(MSG_BUFFER_LENGTH), 
				_executor, 
				new MultiThreadedClaimStrategy(2048), 
				new YieldingWaitStrategy());
		_directSendDisruptor = new Disruptor<>(msgBufferFactory(MSG_BUFFER_LENGTH), 
				_executor, 
				new SingleThreadedClaimStrategy(2048), 
				new YieldingWaitStrategy());
		_pubSendDisruptor = new Disruptor<>(msgBufferFactory(MSG_BUFFER_LENGTH), 
				_executor, 
				new SingleThreadedClaimStrategy(2048), 
				new YieldingWaitStrategy());
		
		//TODO do some lookup
		_clientHandlerId = 0;
		
		EventProcessingHeader header = new EventProcessingHeader(0, 1);
		
		SocketSettings routerSocketSetting = SocketSettings.create(ZMQ.ROUTER)
				.bindToPort(getPort(config.getClientHandlerListenPort()))
				.setMessageOffsets(0, 16);
		MessagePartBufferPolicy routerPolicy = routerSocketSetting.getMessagePartPolicy();
		routerPolicy.addLabel(ClientHandlerProcessor.SOCKET_ID_LABEL, 0);
		routerPolicy.addLabel(ClientHandlerProcessor.CONTENT_LABEL, 1);
		
		SocketSettings updateSocketSetting = SocketSettings.create(ZMQ.SUB)
				.connectToAddress(String.format("tcp://127.0.0.1:%s", config.getCanonicalStatePubPort()))
				.setSocketId(ClientHandlerProcessor.SUB_RECV_SOCKET_ID)
				.setMessageOffsets(0, 0);
		MessagePartBufferPolicy updatePolicy = updateSocketSetting.getMessagePartPolicy();
		updatePolicy.addLabel(ClientHandlerProcessor.SUB_ID_LABEL, 0);
		updatePolicy.addLabel(ClientHandlerProcessor.CONTENT_LABEL, 1);
		SubSocketSettings updateSubSocketSetting = SubSocketSettings.create(updateSocketSetting)
				.subscribeTo(EventType.STATE_UPDATE);
		
		SocketSettings pubSocketSetting = SocketSettings.create(ZMQ.PUB)
				.connectToAddress(String.format("tcp://127.0.0.1:%s", config.getCanonicalSubPort()))
				.setMessageOffsets(0, 0);
		MessagePartBufferPolicy pubPolicy = pubSocketSetting.getMessagePartPolicy();
		
		SequenceBarrier directSendBarrier = _directSendDisruptor.getRingBuffer().newBarrier();
		
		_updateListener = new EventListener(updateSubSocketSetting, 
				_recvDisruptor.getRingBuffer(), 
				_zmqContext, exHandler);
		_directRecvReactor = new RouterSocketReactor(routerSocketSetting, 
				_zmqContext, 
				_recvDisruptor.getRingBuffer(), 
				_directSendDisruptor.getRingBuffer(), 
				directSendBarrier, 
				header, 
				exHandler);
		_processor = new ClientHandlerProcessor(_clientHandlerId, 
				_directSendDisruptor.getRingBuffer(), 
				_pubSendDisruptor.getRingBuffer(), 
				header, 
				header, 
				header, 
				routerPolicy, 
				updatePolicy, 
				pubPolicy);
		_publisher = new Publisher(_zmqContext, pubSocketSetting, header);
		
		_recvDisruptor.handleEventsWith(_processor);
		_pubSendDisruptor.handleEventsWith(_publisher);
		
		_recvDisruptor.start();
		_pubSendDisruptor.start();
		
		_executor.submit(_updateListener);
		_executor.submit(_directRecvReactor);
	}

	@Override
	public void shutdown() {
		_zmqContext.term();
		_executor.shutdownNow();
		try {
			_executor.awaitTermination(5, TimeUnit.SECONDS);
		} catch (InterruptedException eInterrupted) {
			// ignore
		}
	}

	@Override
	public String name() {
		return String.format("Client Handler %d", _clientHandlerId);
	}
	
}
