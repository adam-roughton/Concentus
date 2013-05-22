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
package com.adamroughton.concentus.clienthandler;

import com.adamroughton.concentus.ConcentusExecutableOperations;
import com.adamroughton.concentus.ConcentusExecutableOperations.FactoryDelegate;
import com.adamroughton.concentus.ConcentusHandle;
import com.adamroughton.concentus.ConcentusNode;
import com.adamroughton.concentus.ConcentusProcessFactory;
import com.adamroughton.concentus.NoArgsConcentusProcessFactory;
import com.adamroughton.concentus.config.Configuration;

public class ClientHandlerNode implements ConcentusNode<ClientHandlerService, Configuration> {

	@Override
	public ConcentusProcessFactory<ClientHandlerService, Configuration> getProcessFactory() {
		return new NoArgsConcentusProcessFactory<>(
			"ClientHandlerNode", 
			new FactoryDelegate<ClientHandlerService, Configuration>() {
	
				@Override
				public ClientHandlerService create(
						ConcentusHandle<? extends Configuration> concentusHandle) {
					return new ClientHandlerService(concentusHandle);
				}
			}, 
			ClientHandlerService.class,
			Configuration.class);
	}
	
	public static void main(String[] args) {
		ConcentusExecutableOperations.executeClusterWorker(args, new ClientHandlerNode());
	}
	
}
