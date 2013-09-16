package com.adamroughton.concentus.cluster.worker;

import com.adamroughton.concentus.data.cluster.kryo.ServiceState;

public abstract class ConcentusServiceBase implements ClusterService<ServiceState> {
	
	@Override
	public void onStateChanged(ServiceState newServiceState,
			StateData<ServiceState> stateData, ClusterHandle cluster)
			throws Exception {
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

	protected void onInit(StateData<ServiceState> stateData, ClusterHandle cluster) throws Exception {
	}

	protected void onBind(StateData<ServiceState> stateData, ClusterHandle cluster) throws Exception {
	}

	protected void onConnect(StateData<ServiceState> stateData,
			ClusterHandle cluster) throws Exception {
	}

	protected void onStart(StateData<ServiceState> stateData,
			ClusterHandle cluster) throws Exception {
	}

	protected void onShutdown(StateData<ServiceState> stateData,
			ClusterHandle cluster) throws Exception {
	}



}
