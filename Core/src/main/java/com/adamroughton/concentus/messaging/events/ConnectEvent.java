package com.adamroughton.concentus.messaging.events;

import com.adamroughton.concentus.messaging.ResizingBuffer;

public class ConnectEvent extends BufferBackedObject {

	private final Field isConnectField = super.getBaseField().then(ResizingBuffer.BOOL_SIZE);
	private final Field addressField = isConnectField.thenVariableLength()
			.resolveOffsets();
	
	public ConnectEvent() {
		super(EventType.CONNECT.getId());
	}

	public boolean isConnect() {
		return getBuffer().readBoolean(isConnectField.offset);
	}
	
	public void setIsConnect(boolean isConnect) {
		getBuffer().writeBoolean(isConnectField.offset, isConnect);
	}
	
	public String getAddress() {
		return getBuffer().read8BitCharString(addressField.offset);
	}
	
	public void setAddress(String address) {
		getBuffer().write8BitCharString(addressField.offset, address);
	}
}
