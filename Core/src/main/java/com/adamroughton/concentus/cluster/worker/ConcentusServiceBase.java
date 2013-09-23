package com.adamroughton.concentus.cluster.worker;

import com.adamroughton.concentus.data.cluster.kryo.ServiceState;

public abstract class ConcentusServiceBase implements ClusterService<ServiceState> {
	
	private int _stateChangeIndex;
	
	@Override
	public void onStateChanged(ServiceState newServiceState,
			int stateChangeIndex,
			StateData stateData, 
			ClusterHandle cluster)
			throws Exception {
		_stateChangeIndex = stateChangeIndex;
		switch (newServiceState) {
			case INIT:
				onInit(stateData, cluster);
				break;
			case BIND:
				onBind(stateData, cluster);
				break;
			case CONNECT:
				onConnect(stateData, cluster);
				break;
			case START:
				onStart(stateData, cluster);
				break;
			case SHUTDOWN:
				onShutdown(stateData, cluster);
				break;
			default:
		}
	}
	
	/**
	 * Gets the identity of the current state
	 * @return
	 */
	protected int getStateChangeIndex() {
		return _stateChangeIndex;
	}

	protected void onInit(StateData stateData, ClusterHandle cluster) throws Exception {
	}

	protected void onBind(StateData stateData, ClusterHandle cluster) throws Exception {
	}

	protected void onConnect(StateData stateData,
			ClusterHandle cluster) throws Exception {
	}

	protected void onStart(StateData stateData,
			ClusterHandle cluster) throws Exception {
	}

	protected void onShutdown(StateData stateData,
			ClusterHandle cluster) throws Exception {
	}



}
