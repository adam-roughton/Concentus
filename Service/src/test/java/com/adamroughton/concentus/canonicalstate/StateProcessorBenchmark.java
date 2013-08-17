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
package com.adamroughton.concentus.canonicalstate;

import java.util.concurrent.TimeUnit;

import com.adamroughton.concentus.Clock;
import com.adamroughton.concentus.Constants;
import com.adamroughton.concentus.DefaultClock;
import com.adamroughton.concentus.disruptor.CollocatedBufferEventFactory;
import com.adamroughton.concentus.disruptor.EventQueue;
import com.adamroughton.concentus.disruptor.EventQueueImpl;
import com.adamroughton.concentus.disruptor.SingleProducerQueueStrategy;
import com.adamroughton.concentus.messaging.ArrayBackedResizingBuffer;
import com.adamroughton.concentus.messaging.ArrayBackedResizingBufferFactory;
import com.adamroughton.concentus.messaging.IncomingEventHeader;
import com.adamroughton.concentus.messaging.OutgoingEventHeader;
import com.adamroughton.concentus.messaging.ResizingBuffer;
import com.adamroughton.concentus.messaging.events.StateInputEvent;
import com.adamroughton.concentus.messaging.patterns.SendQueue;
import com.adamroughton.concentus.metric.LogMetricContext;
import com.adamroughton.concentus.metric.NullMetricContext;
import com.lmax.disruptor.Sequence;
import com.lmax.disruptor.YieldingWaitStrategy;

public class StateProcessorBenchmark {

	private Clock _clock;
	private StateProcessor<ArrayBackedResizingBuffer> _stateProcessor;
	private IncomingEventHeader _header;
	private EventQueue<ArrayBackedResizingBuffer> _sendBuffer;
	private StateInputEvent _inputEvent;
	
	private ArrayBackedResizingBuffer _recvBuffer;
	
	public void setUp() {
		int bufferSize = 512;
		
		_clock = new DefaultClock();

		_recvBuffer = new ArrayBackedResizingBuffer(bufferSize);
		_recvBuffer.allocateForWriting(50);
		
		_sendBuffer = new EventQueueImpl<>(new SingleProducerQueueStrategy<>("", 
				new CollocatedBufferEventFactory<>(1, 
				new ArrayBackedResizingBufferFactory(), bufferSize), 
				1,
				new YieldingWaitStrategy()), new NullMetricContext());
		_sendBuffer.addGatingSequences(new Sequence(Long.MAX_VALUE));
		
		_header = new IncomingEventHeader(0, 2);
		
		// set up recvBuffer header content
		_inputEvent = new StateInputEvent();
		_header.setIsValid(_recvBuffer, true);
		int cursor = _header.getEventOffset();
		_header.setSegmentMetaData(_recvBuffer, 0, cursor, 4);
		_recvBuffer.writeInt(cursor, _inputEvent.getTypeId());
		cursor += 4;
		_inputEvent.attachToBuffer(_recvBuffer, cursor);
		_header.setSegmentMetaData(_recvBuffer, 1, cursor, _recvBuffer.getContentSize());
		
		
		SendQueue<OutgoingEventHeader, ArrayBackedResizingBuffer> pubSendQueue = new SendQueue<>("", new OutgoingEventHeader(0, 2), _sendBuffer);
		
		_stateProcessor = new StateProcessor<>(_clock, new StateLogic() {

			@Override
			public void collectInput(ResizingBuffer inputBuffer) {
			}

			@Override
			public void tick(long simTime, long timeDelta) {
			}

			@Override
			public void createUpdate(ResizingBuffer updateBuffer) {
			}
			
		}, _header, pubSendQueue, new LogMetricContext(Constants.METRIC_TICK, TimeUnit.SECONDS.toMillis(Constants.METRIC_BUFFER_SECONDS), _clock));
	}	
	
	public void timeStateProcessor(long inputCount) throws Exception {
		long nextDeadline = _stateProcessor.moveToNextDeadline(0);
		for (long inputSeq = 0; inputSeq < inputCount; inputSeq++) {
			_inputEvent.setInputId(inputSeq);
			_inputEvent.setClientHandlerId((int)(inputSeq % 8));
			
			_stateProcessor.onEvent(_recvBuffer, inputSeq, true);
			if (_clock.currentMillis() >= nextDeadline) {
				_stateProcessor.onDeadline();
				nextDeadline = _stateProcessor.moveToNextDeadline(0);
			}
		}
	}
	
	public long eventSentCount() {
		return _sendBuffer.getCursor();
	}
	
	public static void main(String[] args) {
		long inputCount = 100000000;
		
		for (;;) {
			try {
				StateProcessorBenchmark benchmark = new StateProcessorBenchmark();
				benchmark.setUp();
			
				long startTime = System.nanoTime();
				benchmark.timeStateProcessor(inputCount);
				long duration = System.nanoTime() - startTime;
				
				double sentThroughput = (double) benchmark.eventSentCount() / duration * TimeUnit.SECONDS.toNanos(1);
				double recvThroughput = (double) inputCount / duration * TimeUnit.SECONDS.toNanos(1);
				
				System.out.println(String.format("[%f msgsSent; %f msgsProc]/s", sentThroughput, recvThroughput));
			} catch (Exception e) {
				e.printStackTrace();
			}
		
		}
	}

}
