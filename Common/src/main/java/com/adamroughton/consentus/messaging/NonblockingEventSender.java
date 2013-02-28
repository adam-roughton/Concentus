package com.adamroughton.consentus.messaging;

import org.zeromq.ZMQ;

import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.Sequence;
import com.lmax.disruptor.SequenceBarrier;

/**
 * Handles sending events from the attached ring buffer,
 * not waiting if no events are available to send or the socket
 * is not ready.
 * 
 * @author Adam Roughton
 *
 */
public class NonblockingEventSender {
	
	private final static int DEFAULT_MSG_PART_BUF_SIZE = 32;
	public static final int RESV_OFFSET = 1;
	
	private final RingBuffer<byte[]> _outgoingBuffer;
	private final SequenceBarrier _outgoingBarrier;
	private final Sequence _sequence;
	private long _availableSeq = -1;
	
	private byte[] _msgPartBytesBuffer = new byte[DEFAULT_MSG_PART_BUF_SIZE];
	
	public NonblockingEventSender(final RingBuffer<byte[]> outgoingBuffer,
			final SequenceBarrier outgoingBarrier) {
		_sequence = new Sequence(-1);
		_outgoingBuffer = outgoingBuffer;
		_outgoingBarrier = outgoingBarrier;
	}
	
	/**
	 * Attempts to send a pending event from the outgoing buffer, succeeding
	 * only if both the event is ready and the socket is able to send the event.
	 * @param socket the socket to send the event on
	 * @param msgPartOffsets the policy used to construct message parts from the
	 * ring buffer entry
	 * @return whether an event was sent from the buffer.
	 */
	public boolean sendIfReady(final ZMQ.Socket socket,
			final MessagePartBufferPolicy msgPartOffsets) {
		long nextSeq = _sequence.get() + 1;
		if (nextSeq > _availableSeq) {
			_availableSeq = _outgoingBarrier.getCursor();
		}
		if (nextSeq <= _availableSeq) {
			byte[] outgoingBuffer = _outgoingBuffer.get(nextSeq);
			if (doSend(socket, outgoingBuffer, msgPartOffsets)) {
				_sequence.addAndGet(1);
				return true;
			}
		}
		return false;
	}
	
	private boolean doSend(final ZMQ.Socket socket, 
			byte[] outgoingBuffer,
			final MessagePartBufferPolicy msgPartOffsets) {	
		
		if (msgPartOffsets.getMinReqBufferSize() > outgoingBuffer.length - RESV_OFFSET) {
			throw new IllegalArgumentException(String.format(
					"The message part buffer policy requires a buffer size (%d) greater than" +
					" the underlying buffer of this sender (%d - note %d reserved for flags).", 
					msgPartOffsets.getMinReqBufferSize(),
					outgoingBuffer.length - RESV_OFFSET,
					RESV_OFFSET));
		}
		int partCount = msgPartOffsets.partCount();
		int offset;
		byte[] msgPart;
		boolean success = true;
		int zmqFlag;
		for (int i = 0; i < partCount; i++) {
			// for every msg part that is not the last one
			if (i < partCount - 1) {
				int reqLength = msgPartOffsets.getOffset(i + 1) - msgPartOffsets.getOffset(i);
				msgPart = getBuffer(reqLength);
				offset = msgPart.length - reqLength;
				System.arraycopy(outgoingBuffer, msgPartOffsets.getOffset(i) + RESV_OFFSET, msgPart, offset, reqLength);
				zmqFlag = ZMQ.SNDMORE | ZMQ.NOBLOCK;
			} else {
				offset = msgPartOffsets.getOffset(i) + RESV_OFFSET;
				msgPart = outgoingBuffer;
				zmqFlag = ZMQ.NOBLOCK;
			}
			success = socket.send(msgPart, offset, zmqFlag);
			if (!success)
				return false;
		}
		return success;
	}
	
	public Sequence getSequence() {
		return _sequence;
	}
	
	private byte[] getBuffer(int reqSpace) {
		if (_msgPartBytesBuffer.length < reqSpace) {
			_msgPartBytesBuffer = new byte[reqSpace];
		}
		return _msgPartBytesBuffer;
	}
}