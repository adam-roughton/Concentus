package com.adamroughton.consentus.canonicalstate;

import java.util.Objects;
import com.esotericsoftware.minlog.*;

import org.zeromq.ZMQ;
import org.zeromq.ZMQException;

import com.adamroughton.consentus.Config;
import com.adamroughton.consentus.Util;
import com.adamroughton.consentus.messaging.MessageBytesUtil;
import com.lmax.disruptor.RingBuffer;

class EventListener implements Runnable {
	
	private final ZMQ.Context _zmqContext;
	private final RingBuffer<byte[]> _ringBuffer;
	private final int _listenPort;
	
	public EventListener(ZMQ.Context zmqContext, RingBuffer<byte[]> ringBuffer, Config conf) {
		_zmqContext = Objects.requireNonNull(zmqContext);
		_ringBuffer = Objects.requireNonNull(ringBuffer);
		
		String portString = conf.getCanonicalSubPort();
		_listenPort = Integer.parseInt(portString);
		Util.assertPortValid(_listenPort);
	}

	@Override
	public void run() {		
		ZMQ.Socket input = _zmqContext.socket(ZMQ.SUB);
		input.setHWM(100);
		input.bind("tcp://localhost:" + _listenPort);
		
		try {
			input.subscribe(new byte[0]);
		
			while (!Thread.interrupted()) {
				nextEvent(input);
			}
		} catch (ZMQException eZmq) {
			// check that the socket hasn't just been closed
			if (eZmq.getErrorCode() != ZMQ.Error.ETERM.getCode()) {
				throw eZmq;
			}
		} finally {
			try {
				input.close();
			} catch (Exception eClose) {
				Log.warn("Exception thrown when closing ZMQ socket.", eClose);
			}
		}
	}
	
	private void nextEvent(ZMQ.Socket input) {
		long seq = _ringBuffer.next();
		byte[] array =_ringBuffer.get(seq);
		try {
			// we reserve the first byte of the buffer to communicate
			// whether the event was received correctly
			if (input.recv(array, 1, array.length - 1, 0) == -1) {
				MessageBytesUtil.writeFlagToByte(array, 0, 0, true);
			} else {
				MessageBytesUtil.writeFlagToByte(array, 0, 0, false);
			}	
		} catch (Exception e) {
			// indicate error condition on the message
			MessageBytesUtil.writeFlagToByte(array, 0, 0, true);
			Log.error("An error was raised on receiving a message.", e);
			throw new RuntimeException(e);
		} finally {
			_ringBuffer.publish(seq);
		}
	}
}