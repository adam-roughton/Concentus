package com.adamroughton.concentus.messaging.zmq;

import java.util.Arrays;
import java.util.Map.Entry;
import java.util.Objects;

import it.unimi.dsi.fastutil.objects.Object2ObjectMap;
import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap;

import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Socket;

import com.adamroughton.concentus.Clock;
import com.adamroughton.concentus.data.ArrayBackedResizingBuffer;
import com.adamroughton.concentus.data.BytesUtil;
import com.adamroughton.concentus.data.ResizingBuffer;
import com.adamroughton.concentus.data.events.bufferbacked.ConnectEvent;
import com.adamroughton.concentus.messaging.EventHeader;
import com.adamroughton.concentus.messaging.IncomingEventHeader;
import com.adamroughton.concentus.messaging.Messenger;
import com.adamroughton.concentus.messaging.MessengerClosedException;
import com.adamroughton.concentus.messaging.OutgoingEventHeader;
import com.adamroughton.concentus.messaging.SocketIdentity;
import com.esotericsoftware.minlog.Log;

public final class ZmqDealerSetSocketMessenger implements Messenger<ArrayBackedResizingBuffer> {

	private final Object2ObjectMap<SocketIdentity, ZMQ.Socket> _socketLookup = new Object2ObjectOpenHashMap<>();
	private final Object2ObjectMap<String, ZMQ.Socket> _connStringToSocketLookup = new Object2ObjectOpenHashMap<>();
	private final IdentityLookup _identityLookup;
	private final String _recvRouterAddress;
	private final ZMQ.Context _context;
	private final Clock _clock;
	private final String _name;
			
	private final byte[] _headerBytes = new byte[ResizingBuffer.INT_SIZE];
	
	private final ConnectEvent _recvConnectEvent = new ConnectEvent();
	private final ConnectEvent _sendConnectEvent = new ConnectEvent();
	
	public ZmqDealerSetSocketMessenger(String recvRouterAddress, ZMQ.Context context, Clock clock, String name) {
		_recvRouterAddress = Objects.requireNonNull(recvRouterAddress);
		_context = Objects.requireNonNull(context);
		_clock = Objects.requireNonNull(clock);
		_identityLookup = new IdentityLookup(_clock);
		_name = name;
	}
	
	public void connect(String connString) {
		ArrayBackedResizingBuffer tmpBuffer = new ArrayBackedResizingBuffer(128);
		_recvConnectEvent.attachToBuffer(tmpBuffer, 0);
		_recvConnectEvent.setIsConnect(true);
		_recvConnectEvent.setAddress(connString);
		newConnection(new SocketIdentity(new byte[0]), _recvConnectEvent);
		_recvConnectEvent.releaseBuffer();
	}
	
	public void disconnect(String connString) {
		ArrayBackedResizingBuffer tmpBuffer = new ArrayBackedResizingBuffer(128);
		_recvConnectEvent.attachToBuffer(tmpBuffer, 0);
		_recvConnectEvent.setIsConnect(false);
		_recvConnectEvent.setAddress(connString);
		removeConnection(_recvConnectEvent);
		_recvConnectEvent.releaseBuffer();
	}
	
	@Override
	public boolean send(ArrayBackedResizingBuffer outgoingBuffer,
			OutgoingEventHeader header, boolean isBlocking)
			throws MessengerClosedException {
		// only send if the event is valid
		if (!header.isValid(outgoingBuffer)) return true;	
		
		int segmentCount = header.getSegmentCount();
		if (segmentCount < 2)
			throw new RuntimeException(
					String.format("The event header must have at least 2 segments for the ZmqDealerSetSocket (had %d).", segmentCount));
		
		// read identity
		int identitySegmentMetaData = header.getSegmentMetaData(outgoingBuffer, 0);
		int identityOffset = EventHeader.getSegmentOffset(identitySegmentMetaData);
		int identityLength = EventHeader.getSegmentLength(identitySegmentMetaData);
		SocketIdentity identity = new SocketIdentity(outgoingBuffer.getBuffer(), identityOffset, identityLength);
		
		if (header.isMessagingEvent(outgoingBuffer)) {
			int eventSegmentMetaData = header.getSegmentMetaData(outgoingBuffer, 1);
			int eventOffset = EventHeader.getSegmentOffset(eventSegmentMetaData);
			_recvConnectEvent.attachToBuffer(outgoingBuffer, eventOffset);
			if (_recvConnectEvent.isConnect()) {
				return newConnection(identity.copyWithNewArray(), _recvConnectEvent);				
			} else {
				return removeConnection(_recvConnectEvent);
			}
		} else {
			// lookup socket
			if (!_socketLookup.containsKey(identity)) {
				Log.warn(String.format("No socket found for identity %s", Arrays.toString(identity.copyWithNewArray().buffer)));
				return false;
			}
			ZMQ.Socket socket = _socketLookup.get(identity);
			
			// check event bounds
			int bufferContentSize = outgoingBuffer.getContentSize();
			int lastSegmentMetaData = header.getSegmentMetaData(outgoingBuffer, segmentCount - 1);
			int lastSegmentOffset = EventHeader.getSegmentOffset(lastSegmentMetaData);
			int lastSegmentLength = EventHeader.getSegmentLength(lastSegmentMetaData);
			int requiredLength = lastSegmentOffset + lastSegmentLength;
			if (requiredLength > bufferContentSize) {
				header.setSentTime(outgoingBuffer, -1);
				throw new RuntimeException(String.format("The buffer length is less than the content length (%d < %d)", 
						bufferContentSize, requiredLength));
			}
			
			int startSegmentIndex;
			if (header.isPartiallySent(outgoingBuffer)) {
				startSegmentIndex = header.getNextSegmentToSend(outgoingBuffer);
			} else {
				startSegmentIndex = 1;
			}
			
			if (startSegmentIndex == 1) {
				if (!sendHeader(socket, bufferContentSize - header.getEventOffset(), outgoingBuffer, header, false))
					return false;
			}
			
			int lastSegmentIndex = segmentCount - 1;
			int currentSegmentIndex = ZmqSocketOperations.sendSegments(socket, outgoingBuffer, header, startSegmentIndex, lastSegmentIndex, false);
			if (currentSegmentIndex != lastSegmentIndex) {
				header.setSentTime(outgoingBuffer, -1);
				header.setNextSegmentToSend(outgoingBuffer, currentSegmentIndex);
				header.setIsPartiallySent(outgoingBuffer, true);
				return false;
			}
			header.setSentTime(outgoingBuffer, _clock.currentMillis());
			return true;
		}
	}
	
	public boolean sendHeader(ZMQ.Socket socket, int msgSize, ArrayBackedResizingBuffer eventBuffer, 
			OutgoingEventHeader header, boolean isBlocking) {
		BytesUtil.writeInt(_headerBytes, 0, msgSize);
		return ZmqSocketOperations.doSend(socket, _headerBytes, 0, _headerBytes.length, (isBlocking? 0 : ZMQ.NOBLOCK) | ZMQ.SNDMORE);
	}
	
	private boolean newConnection(SocketIdentity identity, ConnectEvent connectEvent) {		
		ZMQ.Socket socket;
		String address = connectEvent.getAddress();
		
		// check if socket already created but unassigned
		if (_connStringToSocketLookup.containsKey(address)) {
			socket = _connStringToSocketLookup.get(address);
		} else {
			socket = _context.socket(ZMQ.DEALER);
			socket.setLinger(0);
			socket.connect(address);
			
			// pair the other party with this one
			ArrayBackedResizingBuffer tmpBuffer = new ArrayBackedResizingBuffer(128);
			_sendConnectEvent.attachToBuffer(tmpBuffer, 0);
			_sendConnectEvent.writeTypeId();
			_sendConnectEvent.setIsConnect(true);
			_sendConnectEvent.setAddress(_recvRouterAddress);
			_sendConnectEvent.releaseBuffer();
			
			int length = tmpBuffer.getContentSize();
			BytesUtil.writeInt(_headerBytes, 0, length);
			socket.send(_headerBytes, ZMQ.SNDMORE);
			socket.send(tmpBuffer.getBuffer(), 0, length, 0);
		}
		
		if (identity.length == 0) {
			_connStringToSocketLookup.put(address, socket);
		} else if (_socketLookup.containsKey(identity)) {
			Log.warn(String.format("A socket is already registered for identity '%s'", Arrays.toString(identity.buffer)));
			socket.close();
		} else {
			_socketLookup.put(identity, socket);
			_identityLookup.putIdentity(connectEvent.getAddress(), identity);
		}
		return true;
	}
	
	private boolean removeConnection(ConnectEvent disconnectEvent) {
		String address = disconnectEvent.getAddress();
		ZMQ.Socket socket = _connStringToSocketLookup.get(address);
		if (socket == null) {
			Log.warn(String.format("Tried to disconnect from %s when no such connection existed", address));
			return true;
		}
		for (Entry<SocketIdentity, Socket> entry : _socketLookup.entrySet()) {
			if (entry.getValue() == socket) {
				_socketLookup.remove(entry.getKey());
				break;
			}
		}
		socket.close();
		_connStringToSocketLookup.remove(address);
		return true;
	}
	
	@Override
	public boolean recv(ArrayBackedResizingBuffer eventBuffer,
			IncomingEventHeader header, boolean isBlocking)
			throws MessengerClosedException {
		return false;
	}

	@Override
	public int[] getEndpointIds() {
		return new int[0];
	}
	
	public void close() {
		for (ZMQ.Socket socket : _socketLookup.values()) {
			socket.close();
		}
		for (ZMQ.Socket socket : _connStringToSocketLookup.values()) {
			socket.close();
		}
	}

	@Override
	public String name() {
		return _name;
	}
	
	public IdentityLookup getIdentityLookup() {
		return _identityLookup;
	}
	
}
