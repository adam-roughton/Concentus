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
package com.adamroughton.consentus.config;

import java.util.Map;
import java.util.Map.Entry;

public class Service {
	
	private String _name;
	private Map<String, Integer> _ports;
	
	public String getName() {
		return _name;
	}
	
	public void setName(String name) {
		_name = name;
	}
	
	public Map<String, Integer> getPorts() {
		return _ports;
	}
	
	public void setPorts(Map<String, Integer> ports) {
		_ports = ports;
	}
	
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append("Service:\n")
		  .append(String.format("\tname: %s\n", _name))
		  .append("\tports:\n");
		for (Entry<String, Integer> entry : _ports.entrySet()) {
			sb.append(String.format("\t\t%s: %d\n", entry.getKey(), entry.getValue()));
		}
		return sb.toString();
	}
	
}
