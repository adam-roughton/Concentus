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
package com.adamroughton.consentus.crowdhammer.config;

import com.adamroughton.consentus.config.Configuration;

public class CrowdHammerConfiguration extends Configuration {

	private CrowdHammer _crowdHammer;
	
	public CrowdHammer getCrowdHammer() {
		return _crowdHammer;
	}
	
	public void setCrowdHammer(CrowdHammer crowdHammer) {
		_crowdHammer = crowdHammer;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = super.hashCode();
		result = prime * result
				+ ((_crowdHammer == null) ? 0 : _crowdHammer.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (!super.equals(obj))
			return false;
		if (getClass() != obj.getClass())
			return false;
		CrowdHammerConfiguration other = (CrowdHammerConfiguration) obj;
		if (_crowdHammer == null) {
			if (other._crowdHammer != null)
				return false;
		} else if (!_crowdHammer.equals(other._crowdHammer))
			return false;
		return true;
	}
	
}
