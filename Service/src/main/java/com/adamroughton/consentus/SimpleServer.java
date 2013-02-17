package com.adamroughton.consentus;

public class SimpleServer {

	private long _fadeTime;
	
	private long _currentCrowdSize;
	private int[] _currentEmotionValues;
	
	private long _crowdSizeDelta;
	private int[] _emotionInputCount;
	
	private long _simTime;
	private long _gameTick;
	
	private void newEmote(int emotionId, long personCount) {
		_emotionInputCount[emotionId] += personCount;
	}
	
	private void newEmote(int emotionId) {
		newEmote(emotionId, 1);
	}
	
	private void tick() {
		long oldSimTime = _simTime;
		long realClock = System.currentTimeMillis();
		long delta = realClock - oldSimTime;
		_simTime += delta;
		_gameTick++;
		updateState(delta);
	}
	
	private void updateState(long timeDeltaMs) {
		_currentCrowdSize += _crowdSizeDelta;
		
		
		for (int i = 0; i < _currentEmotionValues.length; i++) {
			_currentEmotionValues[i] -= timeDeltaMs;
		}
	}
	
	private int getEmotionDelta(long crowdSize, long personCount) {
		long emoteWeighting = personCount / crowdSize;
		long intensityIncr = emoteWeighting * _fadeTime; // _fadeTime represents the highest intensity for an emotion
		return (int)intensityIncr;
	}
	
	
	
	
	/*
	 * receive input
	 * 
	 * fixed delta for tick
	 * 
	 */
	
}
