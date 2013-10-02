package com.adamroughton.concentus.data.model;


public interface Effect {
	
	long getStartTime();
	
	long getClientIdBits();
	
	ClientId getClientId();
	
	int getVariableId();
	
	int getEffectTypeId();
	
	boolean isCancelled();
	
	/**
	 * A flag indicating whether this effect should be sent
	 * to clients and neighbours.
	 * @return
	 */
	boolean shouldReport();
	
	byte[] getData();
	
}
