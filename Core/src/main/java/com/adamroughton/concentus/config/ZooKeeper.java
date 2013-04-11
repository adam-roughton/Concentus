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
package com.adamroughton.concentus.config;

public class ZooKeeper {
	
	private String _appRoot;
	
	public String getAppRoot() {
		return _appRoot;
	}
	
	public void setAppRoot(String appRoot) {
		_appRoot = appRoot;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result
				+ ((_appRoot == null) ? 0 : _appRoot.hashCode());
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
		ZooKeeper other = (ZooKeeper) obj;
		if (_appRoot == null) {
			if (other._appRoot != null)
				return false;
		} else if (!_appRoot.equals(other._appRoot))
			return false;
		return true;
	}
	
	
}
