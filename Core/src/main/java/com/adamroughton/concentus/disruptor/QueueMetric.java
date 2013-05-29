package com.adamroughton.concentus.disruptor;

import java.util.Objects;

import com.adamroughton.concentus.util.RunningStats;

public final class QueueMetric {

	private final String _queueName;
	private final RunningStats _queueTimeStats = new RunningStats();
	private final RunningStats _queueCapacityStats = new RunningStats();
	
	public QueueMetric(String queueName) {
		_queueName = Objects.requireNonNull(queueName);
	}
	
	public String getQueueName() {
		return _queueName;
	}
	
	public RunningStats getQueueTimeStats() {
		return _queueTimeStats;
	}
	
	public RunningStats getQueueCapacityStats() {
		return _queueCapacityStats;
	}
	
	public void resetStats() {
		_queueTimeStats.reset();
		_queueCapacityStats.reset();
	}
	
	
}
