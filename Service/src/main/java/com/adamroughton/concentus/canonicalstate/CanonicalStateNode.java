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
package com.adamroughton.concentus.canonicalstate;

import com.adamroughton.concentus.ConcentusExecutableOperations;
import com.adamroughton.concentus.ConcentusNode;
import com.adamroughton.concentus.ConcentusProcessFactory;
import com.adamroughton.concentus.NoArgsConcentusProcessFactory;
import com.adamroughton.concentus.ConcentusExecutableOperations.FactoryDelegate;
import com.adamroughton.concentus.ConcentusHandle;
import com.adamroughton.concentus.config.Configuration;

public class CanonicalStateNode implements ConcentusNode<CanonicalStateService, Configuration> {

	@Override
	public ConcentusProcessFactory<CanonicalStateService, Configuration> getProcessFactory() {
		return new NoArgsConcentusProcessFactory<>(
			"ConcentusStateNode", 
			new FactoryDelegate<CanonicalStateService, Configuration>() {
			
				@Override
				public CanonicalStateService create(
						ConcentusHandle<? extends Configuration> concentusHandle) {
					return new CanonicalStateService(concentusHandle);
				}
				
			}, 
		CanonicalStateService.class,
		Configuration.class);
	}
	
	public static void main(String[] args) {
		ConcentusExecutableOperations.executeClusterWorker(args, new CanonicalStateNode());
	}
	
}
