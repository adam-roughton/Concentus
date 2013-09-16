package com.adamroughton.concentus.data.cluster.kryo;

import java.util.Objects;

public final class MetricSourceMetaData {

	private int _metricSourceId;
	private String _sourceName;
	private String _serviceType;
	
	// for Kryo
	@SuppressWarnings("unused")
	private MetricSourceMetaData() { }
	
	public MetricSourceMetaData(int metricSourceId, String sourceName, String serviceType) {
		_metricSourceId = metricSourceId;
		_sourceName = Objects.requireNonNull(sourceName);
		_serviceType = Objects.requireNonNull(serviceType);
	}
	
	public int getMetricSourceId() {
		return _metricSourceId;
	}
	
	public String getSourceName() {
		return _sourceName;
	}
	
	public String getServiceType() {
		return _serviceType;
	}
	
}
