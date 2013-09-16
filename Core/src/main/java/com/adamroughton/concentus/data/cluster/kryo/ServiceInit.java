package com.adamroughton.concentus.data.cluster.kryo;

public class ServiceInit {

	private int _metricSourceId;
	private Object _dataForService;
	
	// for Kryo
	@SuppressWarnings("unused")
	private ServiceInit() { }
	
	public ServiceInit(int metricSourceId, Object dataForService) {
		_metricSourceId = metricSourceId;
		_dataForService = dataForService;
	}
	
	public int getServiceId() {
		return _metricSourceId;
	}
	
	public Object getDataForService() {
		return _dataForService;
	}
	
}
