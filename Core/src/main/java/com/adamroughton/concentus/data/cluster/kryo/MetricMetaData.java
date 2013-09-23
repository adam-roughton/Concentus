package com.adamroughton.concentus.data.cluster.kryo;

import java.util.Objects;

import com.adamroughton.concentus.metric.MetricType;

public final class MetricMetaData {

	private int _metricSourceId;
	private int _metricId;
	private String _reference;
	private String _metricName;
	private MetricType _metricType;
	private boolean _isCumulative;
	
	// for Kryo
	@SuppressWarnings("unused")
	private MetricMetaData() { }
	
	public MetricMetaData(int metricSourceId, int metricId, String reference, 
			String metricName, MetricType metricType, boolean isCumulative) {
		_metricSourceId = metricSourceId;
		_metricId = metricId;
		_reference = Objects.requireNonNull(reference);
		_metricName = Objects.requireNonNull(metricName);
		_metricType = Objects.requireNonNull(metricType);
		_isCumulative = Objects.requireNonNull(isCumulative);
	}
	
	public int getMetricSourceId() {
		return _metricSourceId;
	}
	
	public int getMetricId() {
		return _metricId;
	}
	
	public String getReference() {
		return _reference;
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

	@Override
	public String toString() {
		return "MetricMetaData [metricSourceId=" + _metricSourceId
				+ ", metricId=" + _metricId + ", reference=" + _reference
				+ ", metricName=" + _metricName + ", metricType="
				+ _metricType + ", isCumulative=" + _isCumulative + "]";
	}
	
}
