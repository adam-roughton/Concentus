package com.adamroughton.concentus.data;

import com.esotericsoftware.kryo.Kryo;

public interface KryoRegistratorDelegate {
	
	public final KryoRegistratorDelegate NULL_DELEGATE = new KryoRegistratorDelegate() {
		
		@Override
		public void register(Kryo kryo) {
		}
	};
	
	void register(Kryo kryo);
	
}
