package com.adamroughton.consentus.cluster;

import com.adamroughton.consentus.cluster.worker.ClusterStateValue;

public enum TestState2 implements ClusterStateValue {
	ONE(0, 2),
	TWO(1, 2),
	THREE(2, 2),
	FOUR(3, 2)
	;
	
	private final int _code;
	private final int _domain;
	
	private TestState2(final int code, final int domain) {
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
