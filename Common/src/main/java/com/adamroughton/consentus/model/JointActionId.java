/*
 * Copyright 2013 Adam Roughton
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.adamroughton.consentus.model;

public final class JointActionId {

	private final int _clientHandlerId;
	private final long _actionId;

	public JointActionId(final int clientHandlerId, final long actionId) {
		_clientHandlerId = clientHandlerId;
		_actionId = actionId;
	}
	
	public int getClientHandlerId() {
		return _clientHandlerId;
	}

	public long getActionId() {
		return _actionId;
	}
}
