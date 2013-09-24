package com.adamroughton.concentus.data.cluster.kryo;

import java.util.Objects;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

public final class ProcessReturnInfo implements KryoSerializable {

	public enum ReturnType {
		OK,
		ERROR
	}
	
	private ReturnType _returnType;
	private String _reason;
	
	// for Kryo
	@SuppressWarnings("unused")
	private ProcessReturnInfo() { }
	
	public ProcessReturnInfo(ReturnType returnType, String reason) {
		_returnType = Objects.requireNonNull(returnType);
		_reason = reason;
	}

	public ReturnType getReturnType() {
		return _returnType;
	}
	
	public String getReason() {
		return _reason;
	}
	
	@Override
	public void write(Kryo kryo, Output output) {
		output.writeInt(_returnType.ordinal(), true);
		output.writeString(_reason);
	}

	@Override
	public void read(Kryo kryo, Input input) {
		int ordinal = input.readInt(true);
		ReturnType[] values = ReturnType.values();
		_returnType = values[ordinal];
		_reason = input.readString();
	}

	@Override
	public String toString() {
		return "GuardianDeploymentReturnInfo [returnType=" + _returnType
				+ ", reason=" + _reason + "]";
	}
}
