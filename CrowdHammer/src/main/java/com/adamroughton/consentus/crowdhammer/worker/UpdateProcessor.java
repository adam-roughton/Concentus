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

import com.adamroughton.consentus.crowdhammer.messaging.events.LoadMetricEvent;
import com.adamroughton.consentus.messaging.MessageBytesUtil;
import com.adamroughton.consentus.messaging.events.StateUpdateEvent;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.LifecycleAware;
import com.lmax.disruptor.RingBuffer;

public class UpdateProcessor implements EventHandler<byte[]>, LifecycleAware {

	private final StateUpdateEvent _updateEvent = new StateUpdateEvent();
	private final LoadMetricEvent _loadMetricEvent = new LoadMetricEvent();
	private final RingBuffer<byte[]> _outputRingBuffer;
	
	public UpdateProcessor(final RingBuffer<byte[]> outputRingBuffer) {
		_outputRingBuffer = Objects.requireNonNull(outputRingBuffer);
	}
	
	@Override
	public void onEvent(byte[] event, long sequence, boolean endOfBatch)
			throws Exception {
		if (!isValid(event)) {
			return;
		}
		
		// read in the update
		_updateEvent.setBackingArray(event, 1);
		
		_updateEvent.getUpdateId();
		
		
		_updateEvent.releaseBackingArray();
	}

	@Override
	public void onStart() {
	}

	@Override
	public void onShutdown() {
	}
	
	private static boolean isValid(byte[] event) {
		return !MessageBytesUtil.readFlagFromByte(event, 0, 0);
	}

}
