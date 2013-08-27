package com.adamroughton.concentus.data.model.bufferbacked;

import com.adamroughton.concentus.data.BufferBackedObject;
import com.adamroughton.concentus.data.DataType;
import com.adamroughton.concentus.data.ResizingBuffer;

import static com.adamroughton.concentus.data.ResizingBuffer.*;

public final class CanonicalStateUpdate extends BufferBackedObject {

	private final Field updateIdField = super.getBaseField().then(LONG_SIZE);
	private final Field timeField = updateIdField.then(LONG_SIZE);
	private final Field dataField = timeField.thenVariableLength()
			.resolveOffsets();
	
	public CanonicalStateUpdate() {
		super(DataType.CANONICAL_STATE_UPDATE);
	}
	
	public long getUpdateId() {
		return getBuffer().readLong(updateIdField.offset);
	}
	
	public void setUpdateId(long updateId) {
		getBuffer().writeLong(updateIdField.offset, updateId);
	}
	
	public long getTime() {
		return getBuffer().readLong(timeField.offset);
	}
	
	public void setTime(long time) {
		getBuffer().writeLong(timeField.offset, time);
	}
	
	public ResizingBuffer getData() {
		return getBuffer().slice(dataField.offset);
	}
	

}
