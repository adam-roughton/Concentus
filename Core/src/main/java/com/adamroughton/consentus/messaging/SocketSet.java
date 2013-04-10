package com.adamroughton.consentus.messaging;

import java.util.Arrays;
import java.util.Collection;

import it.unimi.dsi.fastutil.ints.Int2IntArrayMap;
import it.unimi.dsi.fastutil.ints.Int2IntMap;

public class SocketSet {

	private final Int2IntMap _socketLookup;
	private final SocketPackage[] _socketPackages;
	
	public SocketSet(SocketPackage... socketPackages) {
		_socketLookup = new Int2IntArrayMap(socketPackages.length);
		_socketLookup.defaultReturnValue(-1);
		_socketPackages = socketPackages;
		for (int i = 0; i < _socketPackages.length; i++) {
			_socketLookup.put(socketPackages[i].getSocketId(), i);
		}
	}
	
	public final SocketPackage getSocket(int socketId) {
		int socketIndex = _socketLookup.get(socketId);
		if (socketIndex != -1) {
			return _socketPackages[socketIndex];
		} else {
			return null;
		}
	}
	
	public final boolean hasSocket(int socketId) {
		return _socketLookup.containsKey(socketId);
	}
	
	protected final SocketPackage[] getSocketPackages() {
		return _socketPackages;
	}
	
	public Collection<SocketPackage> getSockets() {
		return Arrays.asList(_socketPackages);
	}
}
