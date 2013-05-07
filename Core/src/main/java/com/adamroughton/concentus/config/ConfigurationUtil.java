package com.adamroughton.concentus.config;

import java.util.Map;

import com.adamroughton.concentus.Constants;

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
			throw new RuntimeException(String.format("No such port '%s' for service '%s'.", portName, serviceType));
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
