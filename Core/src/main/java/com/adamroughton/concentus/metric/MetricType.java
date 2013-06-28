package com.adamroughton.concentus.metric;

public enum MetricType {
	STATS(0),
	COUNT(1),
	THROUGHPUT(2);
	
	private final int _id;
	
	private MetricType(int id) {
		_id = id;
	}
	
	public int getId() {
		return _id;
	}

}
