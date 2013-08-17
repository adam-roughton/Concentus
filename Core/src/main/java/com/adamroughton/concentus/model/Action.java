package com.adamroughton.concentus.model;

import java.nio.ByteBuffer;

public interface Action {

	long getClientActionId();
	
	void setClientActionId(long clientActionId);
	
	int getVariableId();
	
	int getActionTypeId();
	
	byte[] getData();
	
	ByteBuffer getDataBuffer();
	
	void copyFromData(byte[] dest, int offset, int length);
	
	void copyToData(byte[] src, int offset, int length);
}
