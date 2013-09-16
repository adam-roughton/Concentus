package com.adamroughton.concentus.metric;

import it.unimi.dsi.fastutil.ints.Int2ObjectArrayMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;

public enum MetricType {
	STATS(0),
	COUNT(1),
	THROUGHPUT(2);
	
	private static final Int2ObjectMap<MetricType> _reverseLookup;
	static {
		MetricType[] metricTypes = MetricType.values();
		_reverseLookup = new Int2ObjectArrayMap<>(metricTypes.length);
		for (MetricType type : metricTypes) {
			_reverseLookup.put(type.getId(), type);
		}
	}
	
	public static MetricType reverseLookup(int metricTypeId) {
		return _reverseLookup.get(metricTypeId);
	}
	
	private final int _id;
	
	private MetricType(int id) {
		_id = id;
	}
	
	public int getId() {
		return _id;
	}

}
