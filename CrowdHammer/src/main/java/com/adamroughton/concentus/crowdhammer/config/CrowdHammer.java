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
package com.adamroughton.concentus.crowdhammer.config;

public class CrowdHammer {

	private int _testRunDurationInSeconds;
	
	public int getTestRunDurationInSeconds() {
		return _testRunDurationInSeconds;
	}
	
	public void setTestRunDurationInSeconds(int seconds) {
		_testRunDurationInSeconds = seconds;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + _testRunDurationInSeconds;
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		CrowdHammer other = (CrowdHammer) obj;
		if (_testRunDurationInSeconds != other._testRunDurationInSeconds)
			return false;
		return true;
	}
	
}
