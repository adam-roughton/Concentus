package com.adamroughton.concentus.crowdhammer.metriclistener;

public class WorkerInfo {
	
	private long _workerId;
	private int _clientCount;
	
	public WorkerInfo() {
	}
	
	public WorkerInfo(long workerId, int simClientCount) {
		_workerId = workerId;
		_clientCount = simClientCount;
	}
	
	public long getWorkerId() {
		return _workerId;
	}
	public void setWorkerId(long workerId) {
		_workerId = workerId;
	}
	public int getSimClientCount() {
		return _clientCount;
	}
	public void setSimClientCount(int simClientCount) {
		_clientCount = simClientCount;
	}

}
