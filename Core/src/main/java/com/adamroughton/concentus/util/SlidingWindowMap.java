package com.adamroughton.concentus.util;

public interface SlidingWindowMap<T> {
	
	/**
	 * Gets the data associated with the given index if the index is
	 * within the window. Otherwise returns unspecified data. This call
	 * should always be used with {@link SlidingWindowLongMap#containsIndex(long)}
	 * to ensure the returned data is valid.
	 * @param index the key
	 * @param entryReader the delegate to read the entry with
	 */
	T get(long index);
	
	long getHeadIndex();
	
	boolean remove(long index);
	
	/**
	 * Gets the number of elements currently stored
	 * in the map.
	 * @return
	 */
	int count();
	
	/**
	 * Gets the size of the sliding window.
	 * @return
	 */
	int getLength();
	
	boolean containsIndex(long index);
	
	void clear();
	
}
