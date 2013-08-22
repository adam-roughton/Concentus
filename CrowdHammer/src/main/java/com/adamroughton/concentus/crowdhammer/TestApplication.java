package com.adamroughton.concentus.crowdhammer;

import com.adamroughton.concentus.messaging.ResizingBuffer;
import com.adamroughton.concentus.model.CandidateValue;
import com.adamroughton.concentus.model.CollectiveVariable;
import com.adamroughton.concentus.model.CollectiveVariableSet;
import com.adamroughton.concentus.model.Effect;
import com.adamroughton.concentus.model.CollectiveApplication;
import com.adamroughton.concentus.model.UserEffectSet;

public class TestApplication implements CollectiveApplication {
	
	private final int _socialSignalId = 0;
	
	public TestApplication() {
		
	}
	
	@Override
	public void processAction(UserEffectSet effectSet, int actionTypeId,
			ResizingBuffer actionData) {
		if (actionTypeId == _socialSignalId) {
			effectSet.newEffect(_socialSignalId, 0, actionData);
		}
	}

	@Override
	public CandidateValue apply(Effect effect, long time) {
		return new CandidateValue(0, 100, new byte[0]);
	}

	@Override
	public void createUpdate(ResizingBuffer updateBuffer,
			CollectiveVariableSet collectiveVariableSet) {
		
	}

}
