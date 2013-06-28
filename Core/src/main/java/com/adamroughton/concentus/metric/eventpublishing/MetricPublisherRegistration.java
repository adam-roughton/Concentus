package com.adamroughton.concentus.metric.eventpublishing;

public final class MetricPublisherRegistration {

	private int _sourceId;
	private String _pubAddress;
	private String _metaDataReqAddress;
	
	public int getSourceId() {
		return _sourceId;
	}
	
	public void setSourceId(int sourceId) {
		_sourceId = sourceId;
	}
	
	public String getPubAddress() {
		return _pubAddress;
	}
	
	public void setPubAddress(String pubAddress) {
		_pubAddress = pubAddress;
	}
	
	public String getMetaDataReqAddress() {
		return _metaDataReqAddress;
	}
	
	public void setMetaDataReqAddress(String metaDataReqAddress) {
		_metaDataReqAddress = metaDataReqAddress;
	}	
	
}
