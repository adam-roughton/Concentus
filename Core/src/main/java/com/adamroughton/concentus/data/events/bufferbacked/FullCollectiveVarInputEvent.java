package com.adamroughton.concentus.data.events.bufferbacked;

import com.adamroughton.concentus.data.BufferBackedObject;
import com.adamroughton.concentus.data.ChunkReader;
import com.adamroughton.concentus.data.ChunkWriter;
import com.adamroughton.concentus.data.DataType;
import com.adamroughton.concentus.data.ResizingBuffer;

import static com.adamroughton.concentus.data.ResizingBuffer.*;

public final class FullCollectiveVarInputEvent extends BufferBackedObject {

	private final Field timeField = super.getBaseField().then(LONG_SIZE);
	private final Field collectiveVarDataField = timeField.thenVariableLength()
			.resolveOffsets();
	
	public FullCollectiveVarInputEvent() {
		super(DataType.FULL_COLLECTIVE_VAR_INPUT_EVENT);
	}
	
	public long getTime() {
		return getBuffer().readLong(timeField.offset);
	}
	
	public void setTime(long time) {
		getBuffer().writeLong(timeField.offset, time);
	}
	
	public ChunkReader getCollectiveVariables() {
		return new ChunkReader(getBuffer(), collectiveVarDataField.offset);
	}
	
	public ChunkWriter newCollectiveVariablesWriter() {
		return new ChunkWriter(getBuffer(), collectiveVarDataField.offset);
	}
	
	public ResizingBuffer getCollectiveVariablesSlice() {
		return getBuffer().slice(collectiveVarDataField.offset);
	}
	
}
