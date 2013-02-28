package com.adamroughton.consentus.messaging;

import java.util.Objects;

import org.zeromq.ZMQ;

import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.Sequence;

/**
 * Handles receiving events from passed sockets into the attached
 * {@link RingBuffer} without blocking.
 * 
 * @author Adam Roughton
 *
 */
public class NonblockingEventReceiver {	
	
	public static final int RESV_OFFSET = 1;
	
	private final RingBuffer<byte[]> _incomingBuffer;
	private final Sequence _sequence;
	
	/**
	 * As we might not recv a message during a {@link recvIfReady} call,
	 * but require a claimed sequence to recv (to maximise memory spatial
	 * locality), we track whether claimed sequences on the incoming buffer
	 * have been used (otherwise we claim another).
	 */
	private long _unpubClaimedSeq = -1;
	
	public NonblockingEventReceiver(final RingBuffer<byte[]> incomingBuffer) {
		_incomingBuffer = Objects.requireNonNull(incomingBuffer);
		_sequence = new Sequence(-1);
	}
	
	/**
	 * Attempts to receive an event if an event is immediately available, and there is
	 * space in the ring buffer for it.
	 * @param socket the socket to receive on
	 * @param msgPartOffsets the offsets to apply to the incoming 
	 * event buffer when receiving the event parts. All offsets are relative
	 * to a reserved {@value #RESV_OFFSET} byte offset (i.e. {3, 5, 10} -> {(3 + {@value #RESV_OFFSET}), 
	 * (5 + {@value #RESV_OFFSET}), (10 + {@value #RESV_OFFSET})}).
	 * @return whether an event was placed in the buffer. This will return true even
	 * if the event is corrupt or had an unexpected number of event parts
	 */
	public boolean recvIfReady(final ZMQ.Socket socket, 
			final MessagePartBufferPolicy msgPartOffsets) {
		if (_unpubClaimedSeq == -1 && _incomingBuffer.hasAvailableCapacity(1)) {
			_unpubClaimedSeq = _incomingBuffer.next();					
		}	
		// only recv if slots are available
		if (_unpubClaimedSeq != -1) {
			byte[] incomingBuffer = _incomingBuffer.get(_unpubClaimedSeq);
			if (doRecv(socket, incomingBuffer, msgPartOffsets)) {
				publish();
				return true;
			}
		}
		return false;
	}
	
	/**
	 * Makes a non blocking call to receive an event.
	 * @param socket the socket to receive on
	 * @param eventBuffer the buffer to place the event in
	 * @param msgPartOffsets the offsets to apply
	 * @return whether an event was placed in the buffer
	 * @see #recvIfReady(org.zeromq.ZMQ.Socket, int[])
	 */
	private boolean doRecv(final ZMQ.Socket socket, 
			final byte[] eventBuffer, 
			final MessagePartBufferPolicy msgPartOffsets) {
		int msgOffsetIndex = 0;
		int expMsgParts = msgPartOffsets.partCount();
		int offset;
		boolean isValid = true;
		
		// get the first offset, or use a default if the policy is not valid
		if (msgPartOffsets.getMinReqBufferSize() > eventBuffer.length - RESV_OFFSET) {
			offset = RESV_OFFSET;
			isValid = false;
		} else if (expMsgParts == 0) {
			offset = RESV_OFFSET;
		} else {
			offset = RESV_OFFSET + msgPartOffsets.getOffset(0);
		}
		
		// check if we have any messages waiting
		int recvdAmount = socket.recv(eventBuffer, offset, eventBuffer.length - offset, ZMQ.NOBLOCK);
		if (recvdAmount == 0) {
			return false;
		} else {
			// now we act on the validity of the message
			if (recvdAmount == -1) {
				isValid = false;
			}
			
			while(isValid && socket.hasReceiveMore() && ++msgOffsetIndex < expMsgParts) {
				offset = RESV_OFFSET + msgPartOffsets.getOffset(msgOffsetIndex);
				recvdAmount = socket.recv(eventBuffer, offset, eventBuffer.length - offset, ZMQ.NOBLOCK);
				if (recvdAmount == -1) {
					isValid = false;
				}
			}
			
			if (msgOffsetIndex < expMsgParts - 1) {
				isValid = false;
			}
			
			// capture any additional message parts
			while (socket.hasReceiveMore()) {
				socket.recv(0);
			}
			
			if (isValid) {
				MessageBytesUtil.writeFlagToByte(eventBuffer, 0, 0, false); // indicate valid
			} else {
				MessageBytesUtil.writeFlagToByte(eventBuffer, 0, 0, true); // indicate error
			}
			
			return true;
		}
	}
	
	private void publish() {
		_incomingBuffer.publish(_unpubClaimedSeq);
		_sequence.set(_unpubClaimedSeq);
		_unpubClaimedSeq = -1;
	}
	
	public void tidyUp() {
		if (_unpubClaimedSeq != -1) {
			byte[] incomingBuffer = _incomingBuffer.get(_unpubClaimedSeq);
			MessageBytesUtil.writeFlagToByte(incomingBuffer, 0, 0, true);
			publish();
		}
	}
	
	public Sequence getSequence() {
		return _sequence;
	}
}