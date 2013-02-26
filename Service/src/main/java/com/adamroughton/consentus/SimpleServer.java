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
