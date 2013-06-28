package com.adamroughton.concentus.metric;

import java.util.Objects;

public final class MetricMetaData {

	private final int _metricId;
	private final String _ownerName;
	private final String _metricName;
	private final MetricType _metricType;
	private final boolean _isCumulative;
	
	public MetricMetaData(int metricId, String ownerName, String metricName, MetricType metricType, boolean isCumulative) {
		_metricId = metricId;
		_ownerName = Objects.requireNonNull(ownerName);
		_metricName = Objects.requireNonNull(metricName);
		_metricType = Objects.requireNonNull(metricType);
		_isCumulative = Objects.requireNonNull(isCumulative);
	}
	
	public int getMetricId() {
		return _metricId;
	}
	
	public String getReference() {
		return _ownerName;
	}
	
	public String getMetricName() {
		return _metricName;
	}
	
	public MetricType getMetricType() {
		return _metricType;
	}
	
	public boolean isCumulative() {
		return _isCumulative;
	}
	
}
