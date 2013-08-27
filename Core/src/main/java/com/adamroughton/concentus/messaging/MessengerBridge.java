package com.adamroughton.concentus.messaging;

import java.util.Objects;

import com.adamroughton.concentus.data.BufferFactory;
import com.adamroughton.concentus.data.ResizingBuffer;
import com.adamroughton.concentus.util.Mutex;
import com.adamroughton.concentus.util.Mutex.OwnerDelegate;

public final class MessengerBridge<TBuffer extends ResizingBuffer> implements Runnable {

	public interface BridgeDelegate<TBuffer extends ResizingBuffer> {
		void onMessageReceived(TBuffer recvBuffer, TBuffer sendBuffer, Messenger<TBuffer> sendMessenger, 
				IncomingEventHeader recvHeader, OutgoingEventHeader sendHeader);
	}
	
	private final BridgeDelegate<TBuffer> _delegate;
	private final int _defaultBufferSize;
	private final BufferFactory<TBuffer> _bufferFactory;
	private final Mutex<Messenger<TBuffer>> _recvMessengerMutex;
	private final Mutex<Messenger<TBuffer>> _sendMessengerMutex;
	private final IncomingEventHeader _recvHeader;
	private final OutgoingEventHeader _sendHeader;
	
	public MessengerBridge(
			BridgeDelegate<TBuffer> delegate, 
			int defaultBufferSize,
			BufferFactory<TBuffer> bufferFactory,
			Mutex<Messenger<TBuffer>> recvMessenger, 
			Mutex<Messenger<TBuffer>> sendMessenger,
			IncomingEventHeader recvHeader,
			OutgoingEventHeader sendHeader) {
		_delegate = Objects.requireNonNull(delegate);
		_defaultBufferSize = defaultBufferSize;
		_bufferFactory = Objects.requireNonNull(bufferFactory);
		_recvMessengerMutex = Objects.requireNonNull(recvMessenger);
		_sendMessengerMutex = Objects.requireNonNull(sendMessenger);
		_recvHeader = Objects.requireNonNull(recvHeader);
		_sendHeader = Objects.requireNonNull(sendHeader);
	}

	@Override
	public void run() {
		final TBuffer recvBuffer = _bufferFactory.newInstance(_defaultBufferSize);
		final TBuffer sendBuffer = _bufferFactory.newInstance(_defaultBufferSize);
		_recvMessengerMutex.runAsOwner(new OwnerDelegate<Messenger<TBuffer>>() {

			@Override
			public void asOwner(final Messenger<TBuffer> recvMessenger) {
				_sendMessengerMutex.runAsOwner(new OwnerDelegate<Messenger<TBuffer>>() {

					@Override
					public void asOwner(Messenger<TBuffer> sendMessenger) {
						while (!Thread.interrupted()) {
							if (recvMessenger.recv(recvBuffer, _recvHeader, false)) {
								_delegate.onMessageReceived(recvBuffer, sendBuffer, sendMessenger, _recvHeader, _sendHeader);
								recvBuffer.reset();
								sendBuffer.reset();
							}
						}
					}
				});
			}
		});
	}
		
}
