package com.adamroughton.concentus.clienthandler;

import java.util.Collection;
import java.util.Objects;

import com.adamroughton.concentus.messaging.SocketIdentity;

public class RoundRobinAllocationStrategy implements ActionProcessorAllocationStrategy {
	
	private SocketIdentity[] _actionProcessorRefs;
	private long _nextSeq = 0;
	
	public RoundRobinAllocationStrategy() {
		_actionProcessorRefs = new SocketIdentity[] { new SocketIdentity(new byte[0]) };
	}
	
	public void setActionProcessorRefs(Collection<SocketIdentity> actionProcessorRefs) {
		setActionProcessorRefs(actionProcessorRefs.toArray(new SocketIdentity[actionProcessorRefs.size()]));
	}
	
	public void setActionProcessorRefs(SocketIdentity[] actionProcessorRefs) {
		Objects.requireNonNull(actionProcessorRefs);
		if (_actionProcessorRefs.length > 0) {
			_actionProcessorRefs = actionProcessorRefs;
		}
	}
	
	@Override
	public SocketIdentity allocateClient(long clientId) {
		return _actionProcessorRefs[(int)(_nextSeq++ % _actionProcessorRefs.length)];
	}

}
