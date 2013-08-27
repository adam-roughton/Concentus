package com.adamroughton.concentus.data.events.bufferbacked;

import com.adamroughton.concentus.data.BufferBackedObject;
import com.adamroughton.concentus.data.DataType;
import com.adamroughton.concentus.data.ResizingBuffer;

public class TickEvent extends BufferBackedObject {

	private final Field timeField = super.getBaseField().then(ResizingBuffer.LONG_SIZE)
			.resolveOffsets();
	
	public TickEvent() {
		super(DataType.TICK_EVENT);
	}

	public long getTime() {
		return getBuffer().readLong(timeField.offset);
	}
	
	public void setTime(long time) {
		getBuffer().writeLong(timeField.offset, time);
	}
}
