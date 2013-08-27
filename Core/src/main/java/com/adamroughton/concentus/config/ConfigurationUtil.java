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

import java.util.Map;

import com.adamroughton.concentus.Constants;
import com.esotericsoftware.minlog.Log;

public class ConfigurationUtil {

	public static int getMessageBufferSize(Configuration config, String serviceType, String bufferName) {
		Service service = getService(config, serviceType, false);
		if (service != null) {
			Map<String, Integer> bufferSizes = service.getMessageBufferSizes();
			if (bufferSizes != null && bufferSizes.containsKey(bufferName)) {
				return bufferSizes.get(bufferName);
			}
		}		
		return Constants.DEFAULT_MSG_BUFFER_SIZE;
	}
	
	public static int getPort(Configuration config, String serviceType, String portName) {
		Service service = getService(config, serviceType, true);
		Map<String, Integer> portAssignments = service.getPorts();
		if (portAssignments == null || !portAssignments.containsKey(portName)) {
			Log.info(String.format("Allowing any port for port ID '%s' of service '%s'.", portName, serviceType));
			return -1;
		}
		return portAssignments.get(portName);
	}
	
	private static Service getService(Configuration config, String serviceType, boolean assertExists) {
		Service service = config.getServices().get(serviceType);
		if (assertExists && service == null)
			throw new RuntimeException(String.format("No such service '%s'", serviceType));
		return service;
	}
	
}
