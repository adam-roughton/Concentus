package com.adamroughton.concentus.data.cluster.kryo;

public class ServiceHandle {

	private String _address;
	private String _type;
	
	// for kryo
	@SuppressWarnings("unused")
	private ServiceHandle() { }
	
	public ServiceHandle(String type, String address) {
		_type = type;
		_address = address;
	}
	
	public String getAddress() {
		return _address;
	}
	
	public String getType() {
		return _type;
	}
	
}
