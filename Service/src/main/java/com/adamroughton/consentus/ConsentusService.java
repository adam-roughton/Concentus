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
package com.adamroughton.consentus;

import java.net.InetAddress;

import com.adamroughton.consentus.cluster.worker.ClusterListener;
import com.adamroughton.consentus.config.Configuration;

public interface ConsentusService extends ClusterListener<ConsentusServiceState> {

	void configure(Configuration config, ConsentusProcessCallback exHandler, InetAddress networkAddress);
	
	String name();
	
}
