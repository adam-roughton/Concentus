package com.adamroughton.consentus.crowdhammer.worker;

import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;

import com.adamroughton.consentus.Config;
import com.adamroughton.consentus.FatalExceptionCallback;
import com.adamroughton.consentus.messaging.MessageBytesUtil;
import com.adamroughton.consentus.messaging.events.StateInputEvent;
import com.esotericsoftware.minlog.Log;
import com.lmax.disruptor.RingBuffer;

public class LoadDriver implements Runnable {

	private final AtomicBoolean _running = new AtomicBoolean(false);
	private final StateInputEvent _stateInput = new StateInputEvent();
	
	private final RingBuffer<byte[]> _publishRingBuffer;
	private final FatalExceptionCallback _exCallback;

	private static long EVENT_SPACING = 0;
	
	private long _lastEventTime;
	private long _accumulator;
	private long _inputId;
	
	public LoadDriver(
			RingBuffer<byte[]> publishRingBuffer, 
			FatalExceptionCallback exceptionCallback,
			Config conf) {
		_publishRingBuffer = Objects.requireNonNull(publishRingBuffer);
		_exCallback = Objects.requireNonNull(exceptionCallback);
	}

	@Override
	public void run() {
		if (!_running.compareAndSet(false, true)) {
			throw new IllegalStateException("The state processor can only be started once.");
		}
		
		_lastEventTime = System.currentTimeMillis();
		_accumulator = 0;
		_inputId = 0;
		
		while(!Thread.interrupted()) {
			try {
				sendIfTime();
				long timeUntilNextEvent = getTimeUntilNextEvent();
				if (timeUntilNextEvent > 0) {
					LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(timeUntilNextEvent));
				}				
			} catch (final Throwable e) {
				_exCallback.signalFatalException(e);
			}
		}
	}
	
	private void sendIfTime() {
		long nextEventTime = System.currentTimeMillis();
		if (nextEventTime >= _lastEventTime + EVENT_SPACING) {
			long timeSinceLast = nextEventTime - _lastEventTime;
			_accumulator += timeSinceLast;
			
			long dt = EVENT_SPACING;
			while (_accumulator >= dt) {
				sendEvent();
				_accumulator -= dt;
			}
			_lastEventTime = nextEventTime;
		}
	}
	
	private long getTimeUntilNextEvent() {
		long remainingTime = _lastEventTime + EVENT_SPACING - System.currentTimeMillis();
		if (remainingTime < 0)
			remainingTime = 0;
		return remainingTime;
	}
	
	private void sendEvent() {
		// create input
		long seq = _publishRingBuffer.next();
		byte[] outputBytes = _publishRingBuffer.get(seq);
		try {
			// we reserve the first byte of the buffer as a header
			_stateInput.setBackingArray(outputBytes, 1);
			_stateInput.setClientHandlerId(0);
			_stateInput.setInputId(_inputId++);
			MessageBytesUtil.writeFlagToByte(outputBytes, 0, 0, false); // is valid
		} catch (Exception e) {
			// indicate error condition on the message
			MessageBytesUtil.writeFlagToByte(outputBytes, 0, 0, true);
			Log.error("An error was raised on receiving a message.", e);
			throw new RuntimeException(e);
		} finally {
			_publishRingBuffer.publish(seq);
			_stateInput.clear();
		}
	}

}
