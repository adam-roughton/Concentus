package com.adamroughton.consentus;

import uk.co.real_logic.intrinsics.ComponentFactory;
import uk.co.real_logic.intrinsics.StructuredArray;

/**
 * Variant of {@link SlidingWindowMap} that uses an in-place array of collocated
 * entries (utilising the {@link StructuredArray} object). Instead of placing items
 * into the map, entries are first prepared by calling {@link StructuredSlidingWindowMap#advance()}
 * and then filled using {@link StructuredSlidingWindowMap#get(long)}.
 * @author Adam Roughton
 *
 * @param <T>
 */
public class StructuredSlidingWindowMap<T> {
		
	private final int _mask;
	private final StructuredArray<Entry> _window;
	
	private long _currentIndex = -1;
	
	/**
	 * Creates a new sliding window with the given window length.
	 * @param windowLength the window length, which must be a power of 2
	 */
	public StructuredSlidingWindowMap(final int windowLength, ComponentFactory<T> dataFactory) {
		if (Integer.bitCount(windowLength) != 1)
			throw new IllegalArgumentException("The window length must be a power of 2");
		_mask = windowLength - 1;
		_window = StructuredArray.newInstance(windowLength, Entry.class, new EntryFactory<T>(dataFactory));
	}
	
	public final long advance() {
		int windowIndex = (int) ++_currentIndex & _mask;
		_window.get(windowIndex).isValid = true;
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
		_window.get(_currentIndex & _mask).isValid = true;
		
		long skippedCount = (_currentIndex - prevIndex) - 1;
		if (skippedCount >= _window.getLength()) 
			skippedCount = _window.getLength() - 1;
		if (skippedCount > 0) {
			// go through skipped indices and set each one to invalid
			for (long i = _currentIndex - skippedCount; i < _currentIndex; i++) {
				if (i >= 0) {
					_window.get((int) (i & _mask)).isValid = false;
				}
			}
		} 
	}
	
	/**
	 * Gets the data associated with the given index if the index is
	 * within the window. Otherwise returns unspecified data. This call
	 * should always be used with {@link StructuredSlidingWindowMap#containsIndex(long)}
	 * to ensure the returned data is valid.
	 * @param index the key
	 * @return the data associated with the given index
	 */
	@SuppressWarnings("unchecked")
	public final T get(long index) {
		return (T) _window.get((int)index & _mask).content;
	}
	
	public final long getHeadIndex() {
		return _currentIndex;
	}
	
	public final boolean remove(long index) {
		boolean hasEntry = containsIndex(index);
		if (hasEntry) {
			_window.get(index & _mask).isValid = false;
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
		for (final Object val : _window) {
			if (val != null) count++;
		}
		return count;
	}
	
	/**
	 * Gets the size of the sliding window.
	 * @return
	 */
	public final int windowSize() {
		return (int)_window.getLength();
	}
	
	public final boolean containsIndex(long index) {
		int relIndex = (int) (_currentIndex - index);
		return relIndex >= 0 && relIndex < _window.getLength() && _window.get((int)(index & _mask)).isValid;
	}
	
	// wrapper classes for signalling the validity of entries in the window
	
	private static class Entry {
		public Object content;
		public boolean isValid = false;
	}
	
	private static class EntryFactory<T> implements ComponentFactory<Entry> {

		private final ComponentFactory<T> _wrappedComponentFactory;
		
		public EntryFactory(ComponentFactory<T> wrappedComponentFactory) {
			_wrappedComponentFactory = wrappedComponentFactory;
		}
		
		@Override
		public Entry newInstance(Object[] initArgs) {
			Entry entry = new Entry();
			entry.isValid = false;
			entry.content = _wrappedComponentFactory.newInstance(initArgs);
			return entry;
		}
		
	}
	
}
