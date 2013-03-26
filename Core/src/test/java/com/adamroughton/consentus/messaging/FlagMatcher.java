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
package com.adamroughton.consentus.messaging;

import org.mockito.ArgumentMatcher;

public class FlagMatcher extends ArgumentMatcher<Integer> {

	private final int _flagSet;
	private final boolean _flagsWanted;
	
	public FlagMatcher(final int flagSet, boolean flagsWanted) {
		_flagSet = flagSet;
		_flagsWanted = flagsWanted;
	}
	
	@Override
	public boolean matches(Object argument) {
		if (argument instanceof Integer) {
			int actualFlags = (int) argument;
			if (_flagsWanted){
				return (actualFlags & _flagSet) == _flagSet;
			} else {
				return (actualFlags & _flagSet) == 0;
			}
		} else {
			return false;
		}
	}
}