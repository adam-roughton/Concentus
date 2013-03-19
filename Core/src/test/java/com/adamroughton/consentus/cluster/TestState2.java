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
