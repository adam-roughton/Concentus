package com.adamroughton.concentus.cluster.worker;

import com.adamroughton.concentus.data.cluster.kryo.ClusterState;

public interface ServiceContext<TState extends Enum<TState> & ClusterState> {

	void enterState(TState newState, Object stateData, TState expectedCurrentState);
	
}
