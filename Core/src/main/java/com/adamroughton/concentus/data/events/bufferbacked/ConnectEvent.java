package com.adamroughton.concentus.data.events.bufferbacked;

import com.adamroughton.concentus.data.BufferBackedObject;
import com.adamroughton.concentus.data.DataType;
import com.adamroughton.concentus.data.ResizingBuffer;

public class ConnectEvent extends BufferBackedObject {

	private final Field isConnectField = super.getBaseField().then(ResizingBuffer.BOOL_SIZE);
	private final Field addressField = isConnectField.thenVariableLength()
			.resolveOffsets();
	
	public ConnectEvent() {
		super(DataType.MESSENGER_CONNECT_EVENT);
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
