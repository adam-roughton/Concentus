package com.adamroughton.concentus.clienthandler;

import java.util.ArrayList;
import java.util.Collection;

import org.javatuples.Pair;

import com.adamroughton.concentus.messaging.SocketIdentity;

public class RoundRobinAllocationStrategy implements ActionCollectorAllocationStrategy {
	
	private final ArrayList<Pair<Integer, SocketIdentity>> _actionCollectorAllocations;
	private long _nextSeq = 0;
	
	public RoundRobinAllocationStrategy() {
		_actionCollectorAllocations = new ArrayList<>();
	}
	
	public void setActionCollectorAllocations(Collection<Pair<Integer, SocketIdentity>> actionCollectorRefs) {
		_actionCollectorAllocations.addAll(actionCollectorRefs);
	}
	
	@Override
	public Pair<Integer, SocketIdentity> allocateClient(long clientId) {
		return _actionCollectorAllocations.get((int)(_nextSeq++ % _actionCollectorAllocations.size()));
	}

}
