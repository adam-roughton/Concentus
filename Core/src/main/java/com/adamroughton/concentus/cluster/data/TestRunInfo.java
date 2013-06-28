package com.adamroughton.concentus.cluster.data;

public final class TestRunInfo {

	private final int _runId;
	private final int _clientCount;
	private final long _duration;
	
	public TestRunInfo(int runId, int clientCount, long duration) {
		_runId = runId;
		_clientCount = clientCount;
		_duration = duration;
	}

	public int getRunId() {
		return _runId;
	}

	public int getClientCount() {
		return _clientCount;
	}

	public long getDuration() {
		return _duration;
	}
	
}
