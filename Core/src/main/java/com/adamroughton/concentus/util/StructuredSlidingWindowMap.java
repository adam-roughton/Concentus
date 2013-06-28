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

import java.util.Objects;

import com.adamroughton.concentus.InitialiseDelegate;

import uk.co.real_logic.intrinsics.ComponentFactory;
import uk.co.real_logic.intrinsics.StructuredArray;

/**
 * Variant of {@link SlidingWindowArrayMap} that uses an in-place array of collocated
 * entries (utilising the {@link StructuredArray} object). Instead of placing items
 * into the map, entries are first prepared by calling {@link StructuredSlidingWindowMap#advance()}
 * and then filled using {@link StructuredSlidingWindowMap#get(long)}.
 * @author Adam Roughton
 *
 * @param <T>
 */
public class StructuredSlidingWindowMap<T> implements SlidingWindowMap<T> {
		
	private final int _mask;
	private final boolean[] _contentFlags;
	private final StructuredArray<T> _window;
	private final InitialiseDelegate<T> _initialiseDelegate;
	
	private long _currentIndex = -1;
	
	/**
	 * Creates a new sliding window with the given window length.
	 * @param windowLength the window length, which must be a power of 2
	 */
	public StructuredSlidingWindowMap(final int windowLength, 
			Class<T> dataType,
			ComponentFactory<T> dataFactory, 
			InitialiseDelegate<T> initialiseDelegate) {
		if (Integer.bitCount(windowLength) != 1)
			throw new IllegalArgumentException("The window length must be a power of 2");
		_mask = windowLength - 1;
		_contentFlags = new boolean[windowLength];
		_window = StructuredArray.newInstance(windowLength, dataType, dataFactory);
		_initialiseDelegate = Objects.requireNonNull(initialiseDelegate);
	}
	
	public final long advance() {
		_currentIndex++;
		prepareEntry(_currentIndex);
		return _currentIndex;
	}
	
	public final void advanceTo(long index) {
		if (index < _currentIndex - _window.getLength()) {
			throw new IllegalArgumentException(
					String.format("The index %d is smaller than the smallest index (%d) in the window.", 
					index, _currentIndex- _window.getLength()));
		}
		long prevIndex = _currentIndex;
		_currentIndex = index;
		prepareEntry(_currentIndex);
		
		long skippedCount = (_currentIndex - prevIndex) - 1;
		if (skippedCount >= _window.getLength()) 
			skippedCount = _window.getLength() - 1;
		if (skippedCount > 0) {
			// go through skipped indices and set each one to invalid
			for (long i = _currentIndex - skippedCount; i < _currentIndex; i++) {
				if (i >= 0) {
					_contentFlags[(int) (i & _mask)] = false;
				}
			}
		} 
	}
	
	private void prepareEntry(long index) {
		int windowIndex = (int)index & _mask;
		_initialiseDelegate.initialise(_window.get(windowIndex));
		_contentFlags[windowIndex] = true;
	}
	
	/**
	 * Gets the data associated with the given index if the index is
	 * within the window. Otherwise returns unspecified data. This call
	 * should always be used with {@link StructuredSlidingWindowMap#containsIndex(long)}
	 * to ensure the returned data is valid.
	 * @param index the key
	 * @return the data associated with the given index
	 */
	public final T get(long index) {
		return _window.get((int)index & _mask);
	}
	
	public final long getHeadIndex() {
		return _currentIndex;
	}
	
	public final boolean remove(long index) {
		boolean hasEntry = containsIndex(index);
		if (hasEntry) {
			_contentFlags[(int)index & _mask] = false;
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
		return (int)_window.getLength();
	}
	
	public final boolean containsIndex(long index) {
		return 
			// is less than the current index?
			Math.min(index, _currentIndex) == index &&
			// is within the window?
			Math.max(_currentIndex - _window.getLength() + 1, index) == index
			// entry is not null
			&& _contentFlags[(int)(index & _mask)];
	}
	
	public final void clear() {
		for (int i = 0; i < _window.getLength(); i++) {
			_contentFlags[i] = false;
			_initialiseDelegate.initialise(_window.get(i));
		}
		_currentIndex = -1;
	}

}
