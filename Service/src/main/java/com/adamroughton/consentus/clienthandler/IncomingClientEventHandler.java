package com.adamroughton.consentus.clienthandler;

import java.util.Objects;

import org.zeromq.ZMQ;

import com.adamroughton.consentus.messaging.MessageBytesUtil;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.Sequence;

import static com.adamroughton.consentus.Constants.CLIENT_ID_LENGTH;

/**
 * Handles receiving events from the router socket without blocking.
 * 
 * @author Adam Roughton
 *
 */
class IncomingClientEventHandler {	
	private final RingBuffer<byte[]> _incomingBuffer;
	private final Sequence _sequence;
	
	/**
	 * As we might not recv a message during a {@link recvIfReady} call,
	 * but require a claimed sequence to recv (to maximise memory spatial
	 * locality), we track whether claimed sequences on the incoming buffer
	 * have been used (otherwise we claim another).
	 */
	private long _unpubClaimedSeq = -1;
	
	public IncomingClientEventHandler(final RingBuffer<byte[]> incomingBuffer) {
		_incomingBuffer = Objects.requireNonNull(incomingBuffer);
		_sequence = new Sequence(-1);
	}
	
	/**
	 * Attempts to receive an event if an event is immediately available, and there is
	 * sufficient buffer space for it.
	 * @param socket the socket to receive on
	 * @return whether an event was placed in the buffer. This will return true even
	 * if the event is corrupt or had an unexpected number of event parts
	 */
	public boolean recvIfReady(final ZMQ.Socket socket) {
		if (_unpubClaimedSeq == -1 && _incomingBuffer.hasAvailableCapacity(1)) {
			_unpubClaimedSeq = _incomingBuffer.next();					
		}	
		// only recv if slots are available
		if (_unpubClaimedSeq != -1) {
			byte[] incomingBuffer = _incomingBuffer.get(_unpubClaimedSeq);
			if (doRecv(socket, incomingBuffer)) {
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
	 * @return whether an event was placed in the buffer. This will return true even
	 * if the event is corrupt or had an unexpected number of event parts
	 */
	private boolean doRecv(final ZMQ.Socket socket, final byte[] eventBuffer) {	
		int amountRecvd = socket.recv(eventBuffer, 1, CLIENT_ID_LENGTH, ZMQ.NOBLOCK);
		if (amountRecvd == 0) {
			return false;
		} else {
			boolean setErrorFlag = false;
			if (amountRecvd == -1 || amountRecvd != CLIENT_ID_LENGTH || !socket.hasReceiveMore()) {
				setErrorFlag = true;
			} else {
				// get the event payload
				amountRecvd = socket.recv(eventBuffer, 1 + CLIENT_ID_LENGTH, 
						eventBuffer.length - (1 + CLIENT_ID_LENGTH), 0);
				setErrorFlag = amountRecvd == -1;
			}
			
			// capture any additional message parts
			while (socket.hasReceiveMore()) {
				socket.recv(0);
			}
			
			if (setErrorFlag) {
				MessageBytesUtil.writeFlagToByte(eventBuffer, 0, 0, true); // indicate error
			} else {
				MessageBytesUtil.writeFlagToByte(eventBuffer, 0, 0, false); // indicate valid
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