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
