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
package com.adamroughton.concentus;

import org.junit.*;

import com.adamroughton.concentus.util.SlidingWindowLongMap;

import static org.junit.Assert.*;

public class TestSlidingWindowLongMap {
	
	private SlidingWindowLongMap _window;

	@Before
	public void setUp() {
		_window = new SlidingWindowLongMap(1024);
	}
	
	@Test
	public void add_withinWindow() {
		for (int i = 0; i < _window.getLength(); i++) {
			assertEquals(i, _window.add(i * 1000 * 1000));
			assertEquals(i * 1000 * 1000, _window.getDirect(i));
		}
	}
	
	@Test
	public void add_WrapAround() {
		for (int i = 0; i < 2 * _window.getLength(); i++) {
			assertEquals(i, _window.add(i * 1000 * 1000));
			assertEquals(i * 1000 * 1000, _window.getDirect(i));
		}
	}
	
	@Test
	public void add_FetchLastEntry() {
		for (int i = 0; i < 2 * _window.getLength(); i++) {
			assertEquals(i, _window.add(i * 1000 * 1000));
			if (i - _window.getLength() >= 0) {
				int lastAvailableEntry = i - _window.getLength() + 1;
				assertEquals(lastAvailableEntry * 1000 * 1000, _window.getDirect(lastAvailableEntry));
			}
		}
	}
	
	@Test
	public void add_FetchSecondEntry() {
		for (int i = 0; i < 2 * _window.getLength(); i++) {
			assertEquals(i, _window.add(i * 1000 * 1000));
			if (i - 1 >= 0) {
				int secondEntry = i - 1;
				assertEquals(secondEntry * 1000 * 1000, _window.getDirect(secondEntry));
			}
		}
	}
	
	@Test
	public void add_MidEntry() {
		int midPoint = _window.getLength() / 2 + 1;
		for (int i = 0; i < 2 * _window.getLength(); i++) {
			assertEquals(i, _window.add(i * 1000 * 1000));
			if (i - midPoint >= 0) {
				int midEntry = i - midPoint;
				assertEquals(midEntry * 1000 * 1000, _window.getDirect(midEntry));
			}
		}
	}
	
	@Test
	public void contains_NotInitialised() {		
		assertFalse(_window.containsIndex(325));
	}
	
	@Test
	public void get_NotInitialised() {		
		_window.getDirect(325);
	}
	
	@Test
	public void contains_InFuture() {
		long lastClientEntry = -1;
		for (int i = 0; i < 2 * _window.getLength(); i++) {
			lastClientEntry = _window.add(i * 1000 * 1000);
		}
		assertFalse(_window.containsIndex(lastClientEntry + 324));
	}
	
	@Test
	public void get_InFuture() {
		long lastClientEntry = -1;
		for (int i = 0; i < 2 * _window.getLength(); i++) {
			lastClientEntry = _window.add(i * 1000 * 1000);
		}
		_window.getDirect(lastClientEntry + 324);
	}
	
	@Test
	public void contains_TooFarInPast() {
		long lastClientEntry = -1;
		for (int i = 0; i < 2 * _window.getLength(); i++) {
			lastClientEntry = _window.add(i * 1000 * 1000);
		}
		assertFalse(_window.containsIndex(lastClientEntry - ((3 * _window.getLength()) / 2)));
	}
	
	@Test
	public void get_TooFarInPast() {
		long lastClientEntry = -1;
		for (int i = 0; i < 2 * _window.getLength(); i++) {
			lastClientEntry = _window.add(i * 1000 * 1000);
		}
		_window.getDirect(lastClientEntry - ((3 * _window.getLength()) / 2));
	}
	
	@Test
	public void contains_Negative() {
		for (int i = 0; i < 2 * _window.getLength(); i++) {
			_window.add(i * 1000 * 1000);
		}
		assertFalse(_window.containsIndex(-300));
	}
	
	@Test
	public void get_Negative() {
		for (int i = 0; i < 2 * _window.getLength(); i++) {
			_window.add(i * 1000 * 1000);
		}
		_window.getDirect(-300);
	}
	
	@Test
	public void put_NoGapsWithinWindow() {
		for (int i = 0; i < _window.getLength(); i++) {
			assertEquals(i * 1000 * 1000, _window.put(i, i * 1000 * 1000));
			assertEquals(i * 1000 * 1000, _window.getDirect(i));
			assertTrue(_window.containsIndex(i));
			assertEquals(i, _window.getHeadIndex());
		}
	}
	
	@Test
	public void put_NoGapsWrapAround() {
		for (int i = 0; i < 2 * _window.getLength(); i++) {
			assertEquals(i * 1000 * 1000, _window.put(i, i * 1000 * 1000));
			assertEquals(i * 1000 * 1000, _window.getDirect(i));
			assertTrue(_window.containsIndex(i));
			assertEquals(i, _window.getHeadIndex());
		}
	}
	
	@Test
	public void put_NoGapsFetchLastEntry() {
		for (int i = 0; i < 2 * _window.getLength(); i++) {
			assertEquals(i * 1000 * 1000, _window.put(i, i * 1000 * 1000));
			assertEquals(i, _window.getHeadIndex());
			if (i - _window.getLength() >= 0) {
				int lastAvailableEntry = i - _window.getLength() + 1;
				assertEquals(lastAvailableEntry * 1000 * 1000, _window.getDirect(lastAvailableEntry));
				assertTrue(_window.containsIndex(lastAvailableEntry));
			}
		}
	}
	
	@Test
	public void put_NoGapsFetchSecondEntry() {
		for (int i = 0; i < 2 * _window.getLength(); i++) {
			assertEquals(i * 1000 * 1000, _window.put(i, i * 1000 * 1000));
			assertEquals(i, _window.getHeadIndex());
			if (i - 1 >= 0) {
				int secondEntry = i - 1;
				assertEquals(secondEntry * 1000 * 1000, _window.getDirect(secondEntry));
				assertTrue(_window.containsIndex(secondEntry));
			}
		}
	}
	
	@Test
	public void put_NoGapsMidEntry() {
		int midPoint = _window.getLength() / 2 + 1;
		for (int i = 0; i < 2 * _window.getLength(); i++) {
			assertEquals(i * 1000 * 1000, _window.put(i, i * 1000 * 1000));
			assertEquals(i, _window.getHeadIndex());
			if (i - midPoint >= 0) {
				int midEntry = i - midPoint;
				assertEquals(midEntry * 1000 * 1000, _window.getDirect(midEntry));
				assertTrue(_window.containsIndex(midEntry));
			}
		}
	}
	
	@Test
	public void put_skipFirst() {
		for (int i = 1; i < 2 * _window.getLength(); i++) {
			assertEquals(i * 1000 * 1000, _window.put(i, i * 1000 * 1000));
			assertEquals(i, _window.getHeadIndex());
			assertEquals(i * 1000 * 1000, _window.getDirect(i));			
			if (i == 1) {
				assertFalse(_window.containsIndex(0));
				assertTrue(_window.containsIndex(1));
			} else {
				assertTrue(_window.containsIndex(i));
			}
		}
	}
	
	@Test
	public void put_skipEveryThird() {
		for (int i = 0; i < 2 * _window.getLength(); i++) {
			if (i % 3 == 0) {
				assertEquals(i * 1000 * 1000, _window.put(i, i * 1000 * 1000));
				assertEquals(i, _window.getHeadIndex());
				assertEquals(i * 1000 * 1000, _window.getDirect(i));
				if (i > 0) {
					assertFalse(_window.containsIndex(i - 2));
					assertFalse(_window.containsIndex(i - 1));
					assertTrue(_window.containsIndex(i));
				}
			}
		}
	}
	
	@Test
	public void put_windowSizeGap() {
		for (int i = 0; i < 100 * _window.getLength(); i++) {
			if (i % _window.getLength() == 0) {
				assertEquals(i * 1000 * 1000, _window.put(i, i * 1000 * 1000));
				assertEquals(i, _window.getHeadIndex());
				assertEquals(i * 1000 * 1000, _window.getDirect(i));
				if (i > 0) {
					for (int j = i - _window.getLength() + 1; j <= i; j++) {
						if (j != i) {
							assertFalse(_window.containsIndex(j));
						} else {
							assertTrue(_window.containsIndex(j));
						}
					}
				} else {
					assertTrue(_window.containsIndex(i));
				}
			}
		}
	}
	
	@Test
	public void put_windowSizeGapPlus2() {
		for (int i = 0; i < 100 * _window.getLength(); i++) {
			if (i % (_window.getLength() + 2) == 0) {
				assertEquals(i * 1000 * 1000, _window.put(i, i * 1000 * 1000));
				assertEquals(i, _window.getHeadIndex());
				assertEquals(i * 1000 * 1000, _window.getDirect(i));
				if (i > 0) {
					for (int j = i - _window.getLength(); j <= i; j++) {
						if (j != i) {
							assertFalse(_window.containsIndex(j));
						} else {
							assertTrue(_window.containsIndex(j));
						}
					}
				} else {
					assertTrue(_window.containsIndex(i));
				}
			}
		}
	}
	
	@Test(expected=IllegalArgumentException.class)
	public void put_windowSizeLessThanMin() {
		int lastIndex = 2 * _window.getLength() - 1;
		for (int i = 0; i <= lastIndex; i++) {
			assertEquals(i, _window.add(i * 1000 * 1000));
			assertEquals(i * 1000 * 1000, _window.getDirect(i));
		}
		_window.put(lastIndex - (_window.getLength() + 1), 0);
	}
	
	@Test
	public void put_withinEstablishedWindow() {
		int lastIndex = 2 * _window.getLength() - 1;
		for (int i = 0; i <= lastIndex; i++) {
			assertEquals(i, _window.add(i * 1000 * 1000));
			assertEquals(i * 1000 * 1000, _window.getDirect(i));
		}
		long currentHeadIndex = _window.getHeadIndex();
		_window.put(lastIndex - 1, 5);
		assertTrue(_window.containsIndex(lastIndex - 1));
		assertEquals(5, _window.getDirect(lastIndex - 1));
		assertEquals(currentHeadIndex, _window.getHeadIndex());
	}
	
	@Test
	public void put_minWithinEstablishedWindow() {
		int lastIndex = 2 * _window.getLength() - 1;
		for (int i = 0; i <= lastIndex; i++) {
			assertEquals(i, _window.add(i * 1000 * 1000));
			assertEquals(i * 1000 * 1000, _window.getDirect(i));
		}
		long currentHeadIndex = _window.getHeadIndex();
		_window.put(lastIndex - _window.getLength(), 5);
		assertTrue(_window.containsIndex(lastIndex - _window.getLength() + 1));
		assertEquals(5, _window.getDirect(lastIndex - _window.getLength()));
		assertEquals(currentHeadIndex, _window.getHeadIndex());
	}
	
}
