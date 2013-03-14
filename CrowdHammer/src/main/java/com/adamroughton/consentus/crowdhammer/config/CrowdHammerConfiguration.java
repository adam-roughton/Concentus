package com.adamroughton.consentus.crowdhammer.config;

import com.adamroughton.consentus.config.Configuration;

public class CrowdHammerConfiguration extends Configuration {

	private CrowdHammer _crowdHammer;
	
	public CrowdHammer getCrowdHammer() {
		return _crowdHammer;
	}
	
	public void setCrowdHammer(CrowdHammer crowdHammer) {
		_crowdHammer = crowdHammer;
	}
	
}
