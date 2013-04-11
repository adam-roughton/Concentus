package com.adamroughton.concentus.messaging.events;

public class InputToUpdateLink {

	private final long _updateId;
	private final long _inputActionId;
	
	public InputToUpdateLink(final long updateId, final long inputActionId) {
		_updateId = updateId;
		_inputActionId = inputActionId;
	}
	
	public long getUpdateId() {
		return _updateId;
	}
	
	public long getInputActionId() {
		return _inputActionId;
	}
	
}
