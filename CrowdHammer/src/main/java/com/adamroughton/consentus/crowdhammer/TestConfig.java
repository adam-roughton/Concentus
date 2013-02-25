package com.adamroughton.consentus.crowdhammer;

import com.adamroughton.consentus.Config;

@SuppressWarnings("serial")
public class TestConfig extends Config {
	
	public static String getWorkingDir(Config conf) {
		return (String) conf.get(WORKING_DIR);
	}
	
	public static void setWorkingDir(Config conf, String workingDir) {
		conf.put(WORKING_DIR, workingDir);
	}
	
	public static String getCanonicalSubPort(Config conf) {
		return (String) conf.get(CANONICAL_SERVICE_SUB_PORT);
	}
	
	public static void setCanonicalSubPort(Config conf, String canonicalSubPort) {
		conf.put(CANONICAL_SERVICE_SUB_PORT, canonicalSubPort);
	}
	
	public static String getCanonicalStatePubPort(Config conf) {
		return (String) conf.get(CANONICAL_SERVICE_STATE_PUB_PORT);
	}
	
	public static void setCanonicalStatePubPort(Config conf, String canonicalStatePubPort) {
		conf.put(CANONICAL_SERVICE_STATE_PUB_PORT, canonicalStatePubPort);
	}
	
	public static String getCanonicalMetricsPubPort(Config conf) {
		return (String) conf.get(CANONICAL_SERVICE_METRICS_PUB_PORT);
	}
	
	public static String getClientHandlerListenPort(Config conf) {
		return (String) conf.get(CLIENT_HANDLER_SERVICE_LISTEN_PORT);
	}
	
	public static void setClientHandlerListenPort(Config conf, String clientHandlerListenPort) {
		conf.put(CLIENT_HANDLER_SERVICE_LISTEN_PORT, clientHandlerListenPort);
	}
	
	/**
	 * The name of the test class to use in testing.
	 */
	public static final String TEST_CLASS = "consentus.crowdhammer.testclass";
	
	public static void setTestClass(Config conf, String testClass) {
		conf.put(TEST_CLASS, testClass);
	}
	
	public void setTestClass(String testClass) {
		setTestClass(this, testClass);
	}
	
	public static String getTestClass(Config conf) {
		return (String) conf.get(TEST_CLASS);
	}
	
	public String getTestClass() {
		return getTestClass(this);
	}
	
	/**
	 * The name of the test class to use in testing.
	 */
	public static final String TEST_METRIC_SUB_PORT = "consentus.crowdhammer.testmetricsubport";
	
	public static void setTestMetricSubPort(Config conf, String testMetricSubPort) {
		conf.put(TEST_METRIC_SUB_PORT, testMetricSubPort);
	}
	
	public void setTestMetricSubPort(String testMetricSubPort) {
		setTestMetricSubPort(this, testMetricSubPort);
	}
	
	public static String getTestMetricSubPort(Config conf) {
		return (String) conf.get(TEST_METRIC_SUB_PORT);
	}
	
	public String getTestMetricSubPort() {
		return getTestMetricSubPort(this);
	}
	
}
