package com.adamroughton.concentus.messaging;

import java.util.concurrent.TimeUnit;

import org.zeromq.ZMQ;

public class SocketPollInSet extends SocketSet {

	private final ZMQ.Poller _poller;
	
	public SocketPollInSet(ZMQ.Context context, SocketPackage... socketPackages) {
		super(socketPackages);
		_poller = new ZMQ.Poller(socketPackages.length);
		for (int i = 0; i < socketPackages.length; i++) {
			_poller.register(socketPackages[i].getSocket(), ZMQ.Poller.POLLIN);
		}
	}
	
	public SocketPackage poll() throws InterruptedException {
		while (!Thread.interrupted()) {
			long availableCount = _poller.poll();
			if (availableCount > 0) {
				return getReadySocket();
			}
		}
		throw new InterruptedException();
	}
	
	public SocketPackage pollNoBlock() {
		int availableCount = _poller.poll(0);
		if (availableCount > 0) {
			return getReadySocket();
		} else {
			return null;
		}
	}
	
	public SocketPackage poll(long timeout, TimeUnit unit) throws InterruptedException {
		long timeoutTime = System.nanoTime() + unit.toNanos(timeout);
		while (!Thread.interrupted() && System.nanoTime() < timeoutTime) {
			int availableCount = _poller.poll(unit.toMillis(timeout));
			if (availableCount > 0) {
				return getReadySocket();
			}
		}
		return null;
	}
	
	private SocketPackage getReadySocket() {
		SocketPackage[] socketPackages = getSocketPackages();
		for (int i = 0; i < socketPackages.length; i++) {
			if (_poller.pollin(i)) return socketPackages[i];
		}
		return null;
	}
	
}
