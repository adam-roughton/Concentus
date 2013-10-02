package com.adamroughton.concentus.application;

import com.adamroughton.concentus.crowdhammer.ClientAgent;
import com.adamroughton.concentus.data.events.bufferbacked.ActionEvent;
import com.adamroughton.concentus.data.model.bufferbacked.CanonicalStateUpdate;

public class ClientAgentBase implements ClientAgent {

	@Override
	public void setClientId(long clientIdBits) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public boolean onInputGeneration(ActionEvent actionEvent) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public void onUpdate(CanonicalStateUpdate update) {
		// TODO Auto-generated method stub
		
	}

}
