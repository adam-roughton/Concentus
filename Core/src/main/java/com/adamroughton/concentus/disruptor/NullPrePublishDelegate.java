package com.adamroughton.concentus.disruptor;

public final class NullPrePublishDelegate implements PrePublishDelegate {

	@Override
	public void beforePublish(long sequence) {
	}

}
