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
