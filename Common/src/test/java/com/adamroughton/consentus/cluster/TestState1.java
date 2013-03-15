package com.adamroughton.consentus.cluster;

import com.adamroughton.consentus.cluster.worker.ClusterStateValue;

public enum TestState1 implements ClusterStateValue {
	ONE(0, 1),
	TWO(1, 1),
	THREE(2, 1),
	FOUR(3, 1)
	;
	
	private final int _code;
	private final int _domain;
	
	private TestState1(final int code, final int domain) {
		_code = code;
		_domain = domain;
	}

	@Override
	public int code() {
		return _code;
	}

	@Override
	public int domain() {
		return _domain;
	}

}
