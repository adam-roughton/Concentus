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
package com.adamroughton.concentus;

import java.net.InetAddress;

import com.adamroughton.concentus.config.Configuration;
import com.adamroughton.concentus.disruptor.EventQueueFactory;
import com.adamroughton.concentus.disruptor.MetricTrackingEventQueueFactory;
import com.adamroughton.concentus.disruptor.StandardEventQueueFactory;
import com.adamroughton.concentus.messaging.zmq.SocketManager;
import com.adamroughton.concentus.messaging.zmq.SocketManagerImpl;
import com.adamroughton.concentus.messaging.zmq.TrackingSocketManagerDecorator;
import com.adamroughton.concentus.metric.MetricContext;
import com.adamroughton.concentus.util.Util;

public class ConcentusHandleFactory {

	public static <TConfig extends Configuration> ConcentusHandle<TConfig> createHandle(
			final Clock clock, 
			TConfig config, 
			String zooKeeperAddress, 
			InetAddress nodeAddress, 
			final MetricContext metricContext,
			boolean useTracingComponents) {
		InstanceFactory<SocketManager> socketManagerFactory;
		EventQueueFactory eventQueueFactory;
		if (useTracingComponents) {
			socketManagerFactory = new InstanceFactory<SocketManager>() {
				
				@Override
				public SocketManager newInstance() {
					return new TrackingSocketManagerDecorator(metricContext, new SocketManagerImpl(clock), clock);
				}
			};
			eventQueueFactory = new StandardEventQueueFactory();
		} else {
			socketManagerFactory = new InstanceFactory<SocketManager>() {
				
				@Override
				public SocketManager newInstance() {
					return new SocketManagerImpl(clock);
				}
			};
			eventQueueFactory = new StandardEventQueueFactory();
		}
		eventQueueFactory = new MetricTrackingEventQueueFactory(metricContext, clock);
		
		String zooKeeperRoot = config.getZooKeeper().getAppRoot();
		//TODO move validation into configuration class
		if (!Util.isValidZKRoot(zooKeeperRoot)) {
			throw new RuntimeException(
					String.format("The ZooKeeper App Root '%s' was not a valid root path " +
							"(can be '/' or '/[A-Za-z0-9]+')", zooKeeperRoot));
		}	
	
		return new ConcentusHandle<TConfig>(socketManagerFactory, eventQueueFactory, clock, config, nodeAddress, zooKeeperAddress);
	}

}
