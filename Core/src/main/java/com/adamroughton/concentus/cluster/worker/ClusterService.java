/*
 * Copyright 2013 Adam Roughton
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.adamroughton.concentus.cluster.worker;

import com.adamroughton.concentus.data.cluster.kryo.ClusterState;

public interface ClusterService<TState extends Enum<TState> & ClusterState> {
	
	/**
	 * Invoked when the coordinator signals a state change for this service.
	 * @param newServiceState the state the service should enter
	 * @param stateData a container holding
	 * data that the coordinator assigned to the service
	 * for this state change. The cluster service can communicate
	 * back with the coordinator using the {@link StateData#setDataForCoordinator(Object)}
	 * method.
	 * @param cluster a handle that allows operations to be called
	 * against the cluster
	 * @throws Exception
	 */
	void onStateChanged(TState newServiceState, 
			StateData stateData,
			ClusterHandle cluster) throws Exception;
	
}
