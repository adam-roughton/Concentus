package com.adamroughton.concentus;

import java.util.Objects;

import com.adamroughton.concentus.cluster.worker.ServiceEndpointType;

public enum ConcentusEndpoints implements ServiceEndpointType {
		CLIENT_HANDLER("client_handler"),
		ACTION_PROCESSOR("action_processor"),
		CANONICAL_STATE_PUB("canonical_state_pub")
	;
	private final String _id;
	private ConcentusEndpoints(String id) {
		_id = Objects.requireNonNull(id);
	}
	@Override
	public String getId() {
		return _id;
	}
	
}
