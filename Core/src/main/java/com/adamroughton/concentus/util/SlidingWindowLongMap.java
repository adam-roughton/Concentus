package com.adamroughton.concentus.util;

public class SlidingWindowLongMap {
	
	/**
	 * We reserve one value (-Long.MAX_VALUE) to represent entries
	 * that have not been set.
	 */
	public static final long NULL = -Long.MAX_VALUE;
	
	private final int _mask;
	private final long[] _window;
	
	private long _currentIndex = -1;
	
	/**
	 * Creates a new sliding window with the given window length.
	 * @param windowLength the window length, which must be a power of 2
	 */
	public SlidingWindowLongMap(final int windowLength) {
		if (Integer.bitCount(windowLength) != 1)
			throw new IllegalArgumentException("The window length must be a power of 2");
		_mask = windowLength - 1;
		_window = new long[windowLength];
	}
	
	public final long add(long data) {
		int windowIndex = (int) ++_currentIndex & _mask;
		_window[windowIndex] = data;
		return _currentIndex;
	}
	
	public final long put(long index, long data) {
		if (index < _currentIndex - _window.length) {
			throw new IllegalArgumentException(
					String.format("The index %d is smaller than the smallest index (%d) in the window.", 
					index, _currentIndex- _window.length));
		}
		_window[(int) index & _mask] = data;
		if (index > _currentIndex) {
			long prevIndex = _currentIndex;
			_currentIndex = index;
			long skippedCount = (_currentIndex - prevIndex) - 1;
			if (skippedCount >= _window.length) 
				skippedCount = _window.length - 1;
			if (skippedCount > 0) {
				// go through skipped indices and set each one to invalid
				for (long i = _currentIndex - skippedCount; i < _currentIndex; i++) {
					if (i >= 0) {
						_window[(int) (i & _mask)] = NULL;
					}
				}
			} 
		}
		return data;
	}
	
	/**
	 * Gets the data associated with the given index if the index is
	 * within the window. Otherwise returns unspecified data. This call
	 * should always be used with {@link SlidingWindowLongMap#containsIndex(long)}
	 * to ensure the returned data is valid.
	 * @param index the key
	 * @return the data associated with the given index
	 */
	public final long get(long index) {
		return _window[(int)index & _mask];
	}
	
	public final long getHeadIndex() {
		return _currentIndex;
	}
	
	public final boolean remove(long index) {
		boolean hasEntry = containsIndex(index);
		if (hasEntry) {
			int windowIndex = (int) index & _mask;
			_window[windowIndex] = NULL;
		}
		return hasEntry;
	}
	
	/**
	 * Gets the number of elements currently stored
	 * in the map. This is an O(n) operation
	 * @return
	 */
	public final int size() {
		int count = 0;
		for (final long val : _window) {
			if (val != NULL) count++;
		}
		return count;
	}
	
	/**
	 * Returns a reference to the underlying window.
	 * Any modifications to the values array will
	 * modify the contents of the {@link SlidingWindowLongMap}.
	 * @return a reference to the underlying window
	 */
	public final long[] values() {
		return _window;
	}
	
	/**
	 * Gets the size of the sliding window.
	 * @return
	 */
	public final int windowSize() {
		return _window.length;
	}
	
	public final boolean containsIndex(long index) {
		int relIndex = (int) (_currentIndex - index);
		return relIndex >= 0 && relIndex < _window.length && _window[(int)(index & _mask)] != NULL;
	}
	
}
