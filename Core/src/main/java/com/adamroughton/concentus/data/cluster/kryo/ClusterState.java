package com.adamroughton.concentus.data.cluster.kryo;

public interface ClusterState {
	
	public final int CREATED_STATE_CODE = 0;
	
	int code();
	
}
