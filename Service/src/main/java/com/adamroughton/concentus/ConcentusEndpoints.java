package com.adamroughton.concentus;

import java.util.Objects;

public enum ConcentusEndpoints {
		CLIENT_HANDLER("client_handler"),
		ACTION_COLLECTOR("action_collector"),
		ACTION_COLLECTOR_TICK_SUB("action_collector_tick_sub"),
		CANONICAL_STATE_PUB("canonical_state_pub")
	;
		
	private final String _id;
	private ConcentusEndpoints(String id) {
		_id = Objects.requireNonNull(id);
	}

	public String getId() {
		return _id;
	}
	
}
