package com.adamroughton.consentus;

public interface ConcentusService {
	
	void start(Config config, ConcentusProcessCallback exHandler);
	
	void shutdown();
	
	String name();
	
}
