package com.adamroughton.consentus.cluster.worker;

import java.util.UUID;

public interface Cluster {
	
	void registerService(final String serviceType, final String address);
	
	String getServiceAtRandom(final String serviceType);
	
	String[] getAllServices(final String serviceType);
	
	void requestAssignment(final String serviceType, final byte[] requestBytes);
	
	byte[] getAssignment(final String serviceType);
	
	void deleteAssignmentRequest(final String serviceType);
	
	void signalReady();
	
	UUID getMyId();
	
}
