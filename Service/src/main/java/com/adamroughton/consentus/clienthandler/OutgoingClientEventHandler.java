package com.adamroughton.consentus.clienthandler;

import org.zeromq.ZMQ;

import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.Sequence;
import com.lmax.disruptor.SequenceBarrier;

import static com.adamroughton.consentus.Constants.CLIENT_ID_LENGTH;

/**
 * Handles sending events from the outgoing event buffer,
 * not waiting if no events are available to send.
 * 
 * @author Adam Roughton
 *
 */
class OutgoingClientEventHandler {
	private final RingBuffer<byte[]> _outgoingBuffer;
	private final SequenceBarrier _outgoingBarrier;
	private final Sequence _sequence;
	private long _availableSeq = -1;
	
	private final byte[] _identityBytesBuffer = new byte[CLIENT_ID_LENGTH];
	
	public OutgoingClientEventHandler(final RingBuffer<byte[]> outgoingBuffer,
			final SequenceBarrier outgoingBarrier) {
		_sequence = new Sequence(-1);
		_outgoingBuffer = outgoingBuffer;
		_outgoingBarrier = outgoingBarrier;
	}
	
	/**
	 * Attempts to send a pending event from the outgoing buffer, succeeding
	 * only if both the event is ready and the socket is able to send the event.
	 * @param socket the socket to send the event on
	 * @return whether an event was sent from the buffer.
	 */
	public boolean sendIfReady(final ZMQ.Socket socket) {
		long nextSeq = _sequence.get() + 1;
		if (nextSeq > _availableSeq) {
			_availableSeq = _outgoingBarrier.getCursor();
		}
		if (nextSeq <= _availableSeq) {
			byte[] outgoingBuffer = _outgoingBuffer.get(nextSeq);
			if (doSend(socket, outgoingBuffer)) {
				_sequence.addAndGet(1);
				return true;
			}
		}
		return false;
	}
	
	private boolean doSend(final ZMQ.Socket socket, byte[] outgoingBuffer) {	
		System.arraycopy(outgoingBuffer, 1, _identityBytesBuffer, 0, _identityBytesBuffer.length);
		boolean success;
		success = socket.send(_identityBytesBuffer, 0, ZMQ.SNDMORE | ZMQ.NOBLOCK);
		success = success? socket.send(outgoingBuffer, 1 + _identityBytesBuffer.length, ZMQ.NOBLOCK) : false;
		return success;
	}
	
	public Sequence getSequence() {
		return _sequence;
	}
}