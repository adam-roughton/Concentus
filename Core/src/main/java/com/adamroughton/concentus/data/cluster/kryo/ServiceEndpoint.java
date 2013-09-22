package com.adamroughton.concentus.data.cluster.kryo;

import java.util.Objects;

public final class ServiceEndpoint {

	private String _endpointType;
	private String _ipAddress;
	private int _port;
	
	// for Kryo
	@SuppressWarnings("unused")
	private ServiceEndpoint() {	}
	
	public ServiceEndpoint(String endpointType, String ipAddress, int port) {
		_endpointType = Objects.requireNonNull(endpointType);
		_ipAddress = Objects.requireNonNull(ipAddress);
		_port = port;
	}
	
	public String type() {
		return _endpointType;
	}
	
	public String ipAddress() {
		return _ipAddress;
	}
	
	public int port() {
		return _port;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result
				+ ((_endpointType == null) ? 0 : _endpointType.hashCode());
		result = prime * result
				+ ((_ipAddress == null) ? 0 : _ipAddress.hashCode());
		result = prime * result + _port;
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (!(obj instanceof ServiceEndpoint)) {
			return false;
		}
		ServiceEndpoint other = (ServiceEndpoint) obj;
		if (_endpointType == null) {
			if (other._endpointType != null) {
				return false;
			}
		} else if (!_endpointType.equals(other._endpointType)) {
			return false;
		}
		if (_ipAddress == null) {
			if (other._ipAddress != null) {
				return false;
			}
		} else if (!_ipAddress.equals(other._ipAddress)) {
			return false;
		}
		if (_port != other._port) {
			return false;
		}
		return true;
	}

	@Override
	public String toString() {
		return "ServiceEndpoint [endpointType=" + _endpointType
				+ ", ipAddress=" + _ipAddress + ", port=" + _port + "]";
	}
	
}
