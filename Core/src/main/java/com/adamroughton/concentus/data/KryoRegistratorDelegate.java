package com.adamroughton.concentus.data;

import com.esotericsoftware.kryo.Kryo;

public interface KryoRegistratorDelegate {
	void register(Kryo kryo);
}
