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
package com.adamroughton.consentus;

import java.util.HashMap;

@SuppressWarnings("serial")
public class Config extends HashMap<String, Object> {

	/**
	 * A setting representing a directory in which Consentus has
	 * full access for file based operations.
	 */
	public static final String WORKING_DIR = "consentus.workingDir";
	
	public String getWorkingDir() {
		return getWorkingDir(this);
	}
	
	public static String getWorkingDir(Config conf) {
		return (String) conf.get(WORKING_DIR);
	}
	
	public void setWorkingDir(String workingDir) {
		setWorkingDir(this, workingDir);
	}
	
	public static void setWorkingDir(Config conf, String workingDir) {
		conf.put(WORKING_DIR, workingDir);
	}
	
	/**
	 * The port on which canonical service nodes listen for events.
	 */
	public static final String CANONICAL_SERVICE_SUB_PORT = "consentus.canonicalservice.subport";
	
	public String getCanonicalSubPort() {
		return getCanonicalSubPort(this);
	}
	
	public static String getCanonicalSubPort(Config conf) {
		return (String) conf.get(CANONICAL_SERVICE_SUB_PORT);
	}
	
	public void setCanonicalSubPort(String canonicalSubPort) {
		setCanonicalSubPort(this, canonicalSubPort);
	}
	
	public static void setCanonicalSubPort(Config conf, String canonicalSubPort) {
		conf.put(CANONICAL_SERVICE_SUB_PORT, canonicalSubPort);
	}
	
	/**
	 * The port on which canonical service nodes publish update events.
	 */
	public static final String CANONICAL_SERVICE_STATE_PUB_PORT = "consentus.canonicalservice.statepubport";
	
	public String getCanonicalStatePubPort() {
		return getCanonicalStatePubPort(this);
	}
	
	public static String getCanonicalStatePubPort(Config conf) {
		return (String) conf.get(CANONICAL_SERVICE_STATE_PUB_PORT);
	}
	
	public void setCanonicalStatePubPort(String canonicalStatePubPort) {
		setCanonicalStatePubPort(this, canonicalStatePubPort);
	}
	
	public static void setCanonicalStatePubPort(Config conf, String canonicalStatePubPort) {
		conf.put(CANONICAL_SERVICE_STATE_PUB_PORT, canonicalStatePubPort);
	}
	
	/**
	 * The port on which canonical service nodes publish metrics events.
	 */
	public static final String CANONICAL_SERVICE_METRICS_PUB_PORT = "consentus.canonicalservice.metricspubport";
	
	public String getCanonicalMetricsPubPort() {
		return getCanonicalMetricsPubPort(this);
	}
	
	public static String getCanonicalMetricsPubPort(Config conf) {
		return (String) conf.get(CANONICAL_SERVICE_METRICS_PUB_PORT);
	}
	
	public void setCanonicalMetricsPubPort(String canonicalMetricsPubPort) {
		setCanonicalMetricsPubPort(this, canonicalMetricsPubPort);
	}
	
	public static void setCanonicalMetricsPubPort(Config conf, String canonicalMetricsPubPort) {
		conf.put(CANONICAL_SERVICE_METRICS_PUB_PORT, canonicalMetricsPubPort);
	}
	
	/**
	 * The port on which client handler service nodes listen for events.
	 */
	public static final String CLIENT_HANDLER_SERVICE_LISTEN_PORT = "consentus.clienthandler.port";
	
	public String getClientHandlerListenPort() {
		return getClientHandlerListenPort(this);
	}
	
	public static String getClientHandlerListenPort(Config conf) {
		return (String) conf.get(CLIENT_HANDLER_SERVICE_LISTEN_PORT);
	}
	
	public void setClientHandlerListenPort(String clientHandlerListenPort) {
		setClientHandlerListenPort(this, clientHandlerListenPort);
	}
	
	public static void setClientHandlerListenPort(Config conf, String clientHandlerListenPort) {
		conf.put(CLIENT_HANDLER_SERVICE_LISTEN_PORT, clientHandlerListenPort);
	}
	
}
