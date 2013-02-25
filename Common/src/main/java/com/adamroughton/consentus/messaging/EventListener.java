package com.adamroughton.consentus.messaging;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

import org.zeromq.ZMQ;
import org.zeromq.ZMQException;

import com.adamroughton.consentus.FatalExceptionCallback;
import com.esotericsoftware.minlog.Log;
import com.lmax.disruptor.RingBuffer;

public final class EventListener implements Runnable {
	private final AtomicBoolean _isRunning = new AtomicBoolean(false);
	
	private final ZMQ.Context _zmqContext;
	private final RingBuffer<byte[]> _ringBuffer;
	private final FatalExceptionCallback _exCallback;
	
	private final MultiSocketSettings _multiSocketSettings;
	
	public EventListener(
			final SubSocketSettings subSocketSettings,
			final RingBuffer<byte[]> ringBuffer, 
			final ZMQ.Context zmqContext,
			final FatalExceptionCallback exCallback) {
		this(MultiSocketSettings.beginWith(subSocketSettings), ringBuffer, zmqContext, exCallback);
	}
	
	public EventListener(
			final SocketSettings socketSettings,
			final RingBuffer<byte[]> ringBuffer, 
			final ZMQ.Context zmqContext,
			final FatalExceptionCallback exCallback) {
		this(MultiSocketSettings.beginWith(socketSettings), ringBuffer, zmqContext, exCallback);
	}
	
	public EventListener(
			final MultiSocketSettings multiSocketSettings,
			final RingBuffer<byte[]> ringBuffer, 
			final ZMQ.Context zmqContext,
			final FatalExceptionCallback exCallback) {
		_ringBuffer = Objects.requireNonNull(ringBuffer);
		_zmqContext = Objects.requireNonNull(zmqContext);
		_exCallback = Objects.requireNonNull(exCallback);
		_multiSocketSettings = Objects.requireNonNull(multiSocketSettings);
	}

	@Override
	public void run() {	
		if (!_isRunning.compareAndSet(false, true)) {
			_exCallback.signalFatalException(new RuntimeException("The event listener can only be started once."));
		}
		
		try {
			ZMQ.Socket[] sockets = new ZMQ.Socket[_multiSocketSettings.socketCount()];
			int[][] multiMessagePartOffsets = new int[_multiSocketSettings.socketCount()][];
			for (int i = 0; i < _multiSocketSettings.socketCount(); i++) {
				if (_multiSocketSettings.isSub(i)) {
					sockets[i] = createSubSocket(_multiSocketSettings.getSubSocketSettings(i));
				} else {
					sockets[i] = createSocket(_multiSocketSettings.getSocketSettings(i));
				}
				multiMessagePartOffsets[i] = _multiSocketSettings.getSocketSettings(i).getMessageOffsets();
			}
			
			try {		
				if (sockets.length == 1) {
					doRecvSingleSocket(sockets[0], multiMessagePartOffsets[0]);
				} else {
					doRecvMultiSocket(sockets, multiMessagePartOffsets);
				}
			} catch (ZMQException eZmq) {
				// check that the socket hasn't just been closed
				if (eZmq.getErrorCode() != ZMQ.Error.ETERM.getCode()) {
					throw eZmq;
				}
			} finally {
				try {
					for (int i = 0; i < sockets.length; i++) {
						sockets[i].close();
					}
				} catch (Exception eClose) {
					Log.warn("Exception thrown when closing ZMQ socket.", eClose);
				}
			}
		} catch (Throwable t) {
			_exCallback.signalFatalException(t);
		}
	}
	
	private void doRecvSingleSocket(final ZMQ.Socket socket, final int[] messagePartOffsets) throws Exception {
		while (!Thread.interrupted()) {
			nextEvent(socket, messagePartOffsets);
		}
	}
	
	private void doRecvMultiSocket(ZMQ.Socket[] sockets, final int[][] multiMessagePartOffsets) throws Exception {
		ZMQ.Poller poller = _zmqContext.poller(sockets.length);
		for (int i = 0; i < sockets.length; i++) {
			poller.register(sockets[i], ZMQ.Poller.POLLIN);
		}
		while (!Thread.interrupted()) {
			poller.poll();
			for (int i = 0; i < sockets.length; i++) {
				if (poller.pollin(i)) {
					nextEvent(poller.getSocket(i), multiMessagePartOffsets[i]);
				}
			}
		}
	}
	
	private void nextEvent(final ZMQ.Socket input, final int[] messagePartOffsets) {
		final long seq = _ringBuffer.next();
		final byte[] array = _ringBuffer.get(seq);
		try {
			int result = -1;
			// we reserve the first byte of the buffer to communicate
			// whether the event was received correctly
			int offset = 1;
			for (int i = 0; i < messagePartOffsets.length; i++) {
				offset += messagePartOffsets[i];
				result = input.recv(array, offset, array.length - offset, 0);
				if (result == -1)
					break;
				if (messagePartOffsets.length > i + 1 &&					
						!input.hasReceiveMore()) {
					result = -1;
					break;
				}
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
	
	private ZMQ.Socket createSocket(SocketSettings socketSettings) throws Exception {
		ZMQ.Socket socket = _zmqContext.socket(socketSettings.getSocketType());
		long hwm = socketSettings.getHWM();
		if (hwm != -1) {
			socket.setHWM(hwm);
		}
		for (int port : socketSettings.getPortsToBindTo()) {
			socket.bind("tcp://*:" + port);
		}
		for (String address : socketSettings.getConnectionStrings()) {
			socket.connect(address);
		}
		return socket;
	}
	
	private ZMQ.Socket createSubSocket(SubSocketSettings subSocketSettings) throws Exception {
		ZMQ.Socket socket = createSocket(subSocketSettings.getSocketSettings());
		for (byte[] subId : subSocketSettings.getSubscriptions()) {
			socket.subscribe(subId);
		}
		return socket;
	}
}
