package com.adamroughton.consentus.cluster.coordinator;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

public interface Cluster {
	
	List<byte[]> getAssignmentRequests(String serviceType);
	
	void setAssignment(String serviceType, UUID serviceId, byte[] assignment);
	
	void setState(int state);
	
	void waitForReady() throws InterruptedException;
	
	void waitForReady(long time, TimeUnit unit) throws InterruptedException;
	
	UUID getMyId();
	
}
