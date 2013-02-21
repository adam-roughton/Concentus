package com.adamroughton.consentus;

import java.util.Objects;

import org.zeromq.ZMQ;
import org.zeromq.ZMQException;

import com.adamroughton.consentus.messaging.MessageBytesUtil;
import com.esotericsoftware.minlog.Log;
import com.lmax.disruptor.RingBuffer;

public abstract class EventListenerBase implements Runnable {
	private final ZMQ.Context _zmqContext;
	private final RingBuffer<byte[]> _ringBuffer;
	private final Config _conf;
	private final FatalExceptionCallback _exCallback;
	
	public EventListenerBase(
			final ZMQ.Context zmqContext, 
			final RingBuffer<byte[]> ringBuffer, 
			final Config conf, 
			final FatalExceptionCallback exCallback) {
		_zmqContext = Objects.requireNonNull(zmqContext);
		_ringBuffer = Objects.requireNonNull(ringBuffer);
		_conf = Objects.requireNonNull(conf);
		_exCallback = Objects.requireNonNull(exCallback);
	}

	@Override
	public void run() {		
		try {
			ZMQ.Socket input = doConnect(_zmqContext, _conf);
			try {		
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
		} catch (Throwable t) {
			_exCallback.signalFatalException(t);
		}
	}
	
	protected abstract ZMQ.Socket doConnect(ZMQ.Context zmqContext, Config conf) throws Exception;
	
	private void nextEvent(ZMQ.Socket input) {
		long seq = _ringBuffer.next();
		byte[] array =_ringBuffer.get(seq);
		try {
			// we reserve the first byte of the buffer to communicate
			// whether the event was received correctly
			int result = input.recv(array, 1, array.length - 1, 0);
			if (input.hasReceiveMore()) {
				// assume the first part is the subscription ID
				input.recv(array, 1, array.length - 1, 0);
			}
			
			if (result == -1) {
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
