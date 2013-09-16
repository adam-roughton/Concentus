package com.adamroughton.concentus.cluster.worker;

public interface StateData<TState> {
	
	boolean hasData();
	
	<T> T getData(Class<T> expectedType);
	
	<T> void setDataForCoordinator(T data);
}