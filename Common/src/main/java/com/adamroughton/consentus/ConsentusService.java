package com.adamroughton.consentus;

public interface ConsentusService {
	
	void start(Config config, ConsentusProcessCallback exHandler);
	
	void shutdown();
	
	String name();
	
}
