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
		
		
		_updateEvent.clear();
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
