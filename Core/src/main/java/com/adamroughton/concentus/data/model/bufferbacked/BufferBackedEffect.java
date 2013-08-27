package com.adamroughton.concentus.data.model.bufferbacked;

import com.adamroughton.concentus.data.BufferBackedObject;
import com.adamroughton.concentus.data.DataType;
import com.adamroughton.concentus.data.ResizingBuffer;
import com.adamroughton.concentus.data.model.ClientId;
import com.adamroughton.concentus.data.model.Effect;

import static com.adamroughton.concentus.data.ResizingBuffer.*;

public class BufferBackedEffect extends BufferBackedObject implements Effect {

	private final Field startTimeField = super.getBaseField().then(LONG_SIZE);
	private final Field clientIdField = startTimeField.then(LONG_SIZE);
	private final Field variableIdField = clientIdField.then(INT_SIZE);
	private final Field effectTypeIdField = variableIdField.then(INT_SIZE);
	private final Field isCancelledField = effectTypeIdField.then(BOOL_SIZE);
	private final Field effectDataField = isCancelledField.thenVariableLength()
			.resolveOffsets();
	
	public BufferBackedEffect() {
		super(DataType.EFFECT);
	}

	@Override
	public long getStartTime() {
		return getBuffer().readLong(startTimeField.offset);
	}

	public void setStartTime(long startTime) {
		getBuffer().writeLong(startTimeField.offset, startTime);
	}

	@Override
	public long getClientIdBits() {
		return getBuffer().readLong(clientIdField.offset);
	}

	public void setClientIdBits(long clientIdBits) {
		getBuffer().writeLong(clientIdField.offset, clientIdBits);
	}

	@Override
	public ClientId getClientId() {
		return getBuffer().readClientId(clientIdField.offset);
	}

	public void setClientId(ClientId clientId) {
		getBuffer().writeClientId(clientIdField.offset, clientId);
	}

	@Override
	public int getVariableId() {
		return getBuffer().readInt(variableIdField.offset);
	}

	public void setVariableId(int variableId) {
		getBuffer().writeInt(variableIdField.offset, variableId);
	}

	@Override
	public int getEffectTypeId() {
		return getBuffer().readInt(effectTypeIdField.offset);
	}

	public void setEffectTypeId(int effectTypeId) {
		getBuffer().writeInt(effectTypeIdField.offset, effectTypeId);
	}

	public boolean isCancelled() {
		return getBuffer().readBoolean(isCancelledField.offset);
	}
	
	public void setIsCancelled(boolean isCancelled) {
		getBuffer().writeBoolean(isCancelledField.offset, isCancelled);
	}
	
	@Override
	public byte[] getData() {
		ResizingBuffer buffer = getBuffer();
		return buffer.readBytes(effectDataField.offset, buffer.getContentSize());
	}

	public void setData(byte[] data) {
		getBuffer().writeBytes(effectDataField.offset, data);
	}

	public ResizingBuffer getDataSlice() {
		return getBuffer().slice(effectDataField.offset);
	}
	
}
