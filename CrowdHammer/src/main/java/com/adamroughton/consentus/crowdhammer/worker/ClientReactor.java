package com.adamroughton.consentus.crowdhammer.worker;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;

import org.zeromq.ZMQ;

import com.adamroughton.consentus.messaging.EventProcessingHeader;
import com.adamroughton.consentus.messaging.EventReceiver;
import com.adamroughton.consentus.messaging.EventSender;
import com.adamroughton.consentus.messaging.MessagePartBufferPolicy;
import com.adamroughton.consentus.messaging.events.ClientInputEvent;
import com.adamroughton.consentus.messaging.events.ClientUpdateEvent;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.SequenceBarrier;
import uk.co.real_logic.intrinsics.StructuredArray;

import static com.adamroughton.consentus.Constants.*;

public class ClientReactor implements Runnable {

	private final AtomicBoolean _instanceActive = new AtomicBoolean(false);
	private final StructuredArray<Client> _clients;
	private volatile boolean _isRunning = false;
	private final long _clientLengthMask;
	
	private final RingBuffer<byte[]> _metricSendQueue;
	private final SequenceBarrier _metricSendBarrier;
	private final EventSender _sender;
	private final EventReceiver _receiver;
	
	private final ClientInputEvent _inputEvent = new ClientInputEvent();
	private final ClientUpdateEvent _updateEvent = new ClientUpdateEvent();
	
	public ClientReactor(final StructuredArray<Client> clients, 
			final RingBuffer<byte[]> metricSendQueue,
			final SequenceBarrier metricSendBarrier) {
		
		_clients = Objects.requireNonNull(clients);
		long clientsLength = _clients.getLength();
		if (Long.bitCount(clientsLength) != 1) 
			throw new IllegalArgumentException("The allocated client array must be a power of 2.");
		_clientLengthMask = clientsLength - 1;
		
		_metricSendQueue = Objects.requireNonNull(metricSendQueue);
		_metricSendBarrier = Objects.requireNonNull(metricSendBarrier);
		
		EventProcessingHeader header = new EventProcessingHeader(0, 0);
		_sender = new EventSender(header, false);
		_receiver = new EventReceiver(header, true);
	}
	
	@Override
	public void run() {
		if (_instanceActive.getAndSet(true)) {
			throw new IllegalArgumentException(
					"Only one instance of the client reactor should be started.");
		}
		
		byte[] messageBuffer = new byte[MSG_BUFFER_LENGTH];
		MessagePartBufferPolicy msgPartPolicy = new MessagePartBufferPolicy(0, 4);
		
		long currentClientIndex = 0;
		while(_isRunning) {
//			if (isMetricPubTime) {
//				createMetricEvent;
//			}
			
			// get next client
			Client client = _clients.get(currentClientIndex++ & _clientLengthMask);
			long nextSendTime = client.getNextSendTimeInNanos();
			
			// recv events
			ZMQ.Socket socket = client.getSocket();
			while(_receiver.recv(socket, messageBuffer, msgPartPolicy)) {
				processClientUpdate(messageBuffer);
			}
			
			// send outgoing event
			LockSupport.parkNanos(getWaitTime(nextSendTime));
			createClientEvent(client, messageBuffer);
			_sender.send(socket, messageBuffer, msgPartPolicy);
		}
	}
	
	private void processClientUpdate(byte[] incomingBuffer) {
		// create metrics
	}
	
	private void createClientEvent(final Client client, final byte[] outgoingBuffer) {
		long sendTime = System.nanoTime();
		long actionId = client.addSentAction(sendTime);
		_inputEvent.setBackingArray(outgoingBuffer, 0);
		try {
			_inputEvent.setClientId(client.getClientId());
			
		} finally {
			_inputEvent.releaseBackingArray();
		}
	}
	
	public void stop() {
		_isRunning = false;
	}
	
	private long getWaitTime(long nextSendTime) {
		long remainingTime = nextSendTime - System.nanoTime();
		if (remainingTime < 0)
			remainingTime = 0;
		return remainingTime;
	}
	
}
