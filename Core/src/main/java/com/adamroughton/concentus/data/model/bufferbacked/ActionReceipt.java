package com.adamroughton.concentus.data.model.bufferbacked;

import static com.adamroughton.concentus.data.ResizingBuffer.LONG_SIZE;

import com.adamroughton.concentus.data.BufferBackedObject;
import com.adamroughton.concentus.data.DataType;

public final class ActionReceipt extends BufferBackedObject  {

	private final Field actionIdField = super.getBaseField().then(LONG_SIZE);
	private final Field startTimeField = actionIdField.then(LONG_SIZE)
			.resolveOffsets();
	
	public ActionReceipt() {
		super(DataType.ACTION_RECEIPT);
	}
	
	public long getActionId() {
		return getBuffer().readLong(actionIdField.offset);
	}
	
	public void setActionId(long actionId) {
		getBuffer().writeLong(actionIdField.offset, actionId);
	}
	
	public long getStartTime() {
		return getBuffer().readLong(startTimeField.offset);
	}
	
	public void setStartTime(long startTime) {
		getBuffer().writeLong(startTimeField.offset, startTime);
	}
}
