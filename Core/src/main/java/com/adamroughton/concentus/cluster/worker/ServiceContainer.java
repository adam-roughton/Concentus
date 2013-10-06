package com.adamroughton.concentus.cluster.worker;

import java.io.Closeable;

public interface ServiceContainer extends Closeable {

	void start();
	
}
