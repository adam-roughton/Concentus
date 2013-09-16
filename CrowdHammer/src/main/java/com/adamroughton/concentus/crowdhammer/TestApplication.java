package com.adamroughton.concentus.crowdhammer;

import it.unimi.dsi.fastutil.ints.Int2ObjectMap;

import com.adamroughton.concentus.data.ChunkWriter;
import com.adamroughton.concentus.data.ResizingBuffer;
import com.adamroughton.concentus.data.model.Effect;
import com.adamroughton.concentus.data.model.kryo.CandidateValue;
import com.adamroughton.concentus.data.model.kryo.CollectiveVariable;
import com.adamroughton.concentus.model.CollectiveVariableDefinition;
import com.adamroughton.concentus.model.CollectiveApplication;
import com.adamroughton.concentus.model.UserEffectSet;

public class TestApplication implements CollectiveApplication {

	private static final String[] PHRASES = new String[] { "Horay!", "Awesome", "Hmmmmm", "What?!", "That really sucks" };
	
	private long _seq = 0;
	
	@Override
	public void processAction(UserEffectSet effectSet, int actionTypeId,
			ResizingBuffer actionData) {
		if (actionTypeId == 0) {
			byte[] data = PHRASES[(int)((_seq++ / 1000) % PHRASES.length)].getBytes();
			effectSet.newEffect(0, 0, data);
		}		
	}

	@Override
	public CandidateValue apply(Effect effect, long time) {
		long timeActive = time - effect.getStartTime();
		int score;
		if (timeActive < 10000) 
			score = 100;
		else if (timeActive < 20000) 
			score = (int) Math.ceil(100 * (timeActive - 10000) / 900);
		else
		 	score = 0;
		return new CandidateValue(effect.getVariableId(), score, effect.getData());
	}

	@Override
	public long getTickDuration() {
		return 1000;
	}

	@Override
	public CollectiveVariableDefinition[] variableDefinitions() {
		return new CollectiveVariableDefinition[] {
				new CollectiveVariableDefinition(0, 50)
		};
	}

	@Override
	public void createUpdate(ResizingBuffer updateData, long time,
			Int2ObjectMap<CollectiveVariable> collectiveVariableSet) {
		ChunkWriter chunkWriter = new ChunkWriter(updateData);
		ResizingBuffer chunkBuffer = chunkWriter.getChunkBuffer();
		
		CollectiveVariable topN = collectiveVariableSet.get(0);
		for (int i = 0; i < topN.getValueCount(); i++) {
			CandidateValue val = topN.getValue(i);
			chunkBuffer.writeInt(0, val.getScore());
			chunkBuffer.writeBytes(ResizingBuffer.INT_SIZE, val.getValueData());
			chunkWriter.commitChunk();
		}
		chunkWriter.finish();
	}

}
