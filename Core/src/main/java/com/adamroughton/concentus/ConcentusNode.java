package com.adamroughton.concentus;

import com.adamroughton.concentus.config.Configuration;

public interface ConcentusNode<TProcess, TConfig extends Configuration> {
	
	ConcentusProcessFactory<TProcess, TConfig> getProcessFactory();
	
}
