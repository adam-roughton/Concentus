package com.adamroughton.consentus.messaging;

import org.zeromq.ZMQ;

public class SocketPollInSet {

	private final ZMQ.Poller _poller;
	private final SocketPackage[] _socketPackages;
	
	public SocketPollInSet(ZMQ.Context context, SocketPackage... socketPackages) {
		_poller = new ZMQ.Poller(socketPackages.length);
		_socketPackages = socketPackages;
		for (int i = 0; i < _socketPackages.length; i++) {
			_poller.register(_socketPackages[i].getSocket());
		}
	}
	
	public SocketPackage poll() throws InterruptedException {
		while (!Thread.interrupted()) {
			_poller.poll();
			for (int i = 0; i < _socketPackages.length; i++) {
				if (_poller.pollin(i)) return _socketPackages[i];
			}
		}
		throw new InterruptedException();
	}
	
}
