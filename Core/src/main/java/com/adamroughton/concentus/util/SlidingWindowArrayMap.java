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
package com.adamroughton.concentus.util;

public class SlidingWindowArrayMap<T> implements SlidingWindowMap<T> {
		
	private final int _mask;
	private final Object[] _window;
	
	private long _currentIndex = -1;
	
	/**
	 * Creates a new sliding window with the given window length.
	 * @param windowLength the window length, which must be a power of 2
	 */
	public SlidingWindowArrayMap(final int windowLength) {
		if (Integer.bitCount(windowLength) != 1)
			throw new IllegalArgumentException("The window length must be a power of 2");
		_mask = windowLength - 1;
		_window = new Object[windowLength];
	}
	
	public final long add(T data) {
		int windowIndex = (int) ++_currentIndex & _mask;
		_window[windowIndex] = data;
		return _currentIndex;
	}
	
	public final T put(long index, T data) {
		if (index < _currentIndex - _window.length) {
			throw new IllegalArgumentException(
					String.format("The index %d is smaller than the smallest index (%d) in the window.", 
					index, _currentIndex- _window.length));
		}
		long prevIndex = _currentIndex;
		_currentIndex = index;
		int windowIndex = (int) _currentIndex & _mask;
		_window[windowIndex] = data;
		
		long skippedCount = (_currentIndex - prevIndex) - 1;
		if (skippedCount >= _window.length) 
			skippedCount = _window.length - 1;
		if (skippedCount > 0) {
			// go through skipped indices and set each one to invalid
			for (long i = _currentIndex - skippedCount; i < _currentIndex; i++) {
				if (i >= 0) {
					_window[(int) (i & _mask)] = null;
				}
			}
		} 
		return data;
	}
	
	/**
	 * Gets the data associated with the given index if the index is
	 * within the window. Otherwise returns unspecified data. This call
	 * should always be used with {@link SlidingWindowArrayMap#containsIndex(long)}
	 * to ensure the returned data is valid.
	 * @param index the key
	 * @return the data associated with the given index
	 */
	@SuppressWarnings("unchecked")
	public final T get(long index) {
		return (T)_window[(int)index & _mask];
	}
	
	public final long getHeadIndex() {
		return _currentIndex;
	}
	
	public final boolean remove(long index) {
		boolean hasEntry = containsIndex(index);
		if (hasEntry) {
			int windowIndex = (int) index & _mask;
			_window[windowIndex] = null;
		}
		return hasEntry;
	}
	
	/**
	 * Gets the number of elements currently stored
	 * in the map. This is an O(n) operation
	 * @return
	 */
	public final int count() {
		int count = 0;
		for (final Object val : _window) {
			if (val != null) count++;
		}
		return count;
	}
	
	/**
	 * Gets the size of the sliding window.
	 * @return
	 */
	public final int getLength() {
		return _window.length;
	}
	
	public final boolean containsIndex(long index) {
		return 
			// is less than the current index?
			Math.min(index, _currentIndex) == index &&
			// is within the window?
			Math.max(_currentIndex - _window.length + 1, index) == index
			// entry is not null
			&& _window[(int)(index & _mask)] != null;
	}
	
	public final void clear() {
		for (int i = 0; i < _window.length; i++) {
			_window[i] = null;
		}
		_currentIndex = -1;
	}
	
}
