package com.adamroughton.concentus.cluster;

import java.util.Objects;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import com.adamroughton.concentus.util.IdentityWrapper;

public class VersioningListenableContainer<TState, TListener> implements VersioningListenable<TListener> {
	
	public static interface ListenerInvokeDelegate<TState, TListener> {
		void invoke(TListener listener, TState state, int updateVersion);
	}
	
	private final AtomicInteger _version = new AtomicInteger(0);
	private final AtomicReference<VersionEntry<TState>> _state;
	private final ListenerInvokeDelegate<TState, TListener> _listenerInvokeDelegate;
	private final Executor _defaultListenerExecutor = Executors.newSingleThreadExecutor();
	private final ConcurrentMap<IdentityWrapper<TListener>, ListenerEntry> _listeners = new ConcurrentHashMap<>();

	public VersioningListenableContainer(ListenerInvokeDelegate<TState, TListener> listenerInvokeDelegate) {
		_state = new AtomicReference<>(new VersionEntry<TState>(null, -1));
		_listenerInvokeDelegate = Objects.requireNonNull(listenerInvokeDelegate);
	}
	
	@Override
	public void removeListener(TListener listener) {
		_listeners.remove(new IdentityWrapper<>(listener));
	}
	
	@Override
	public void addListener(TListener listener, Executor executor) {
		Objects.requireNonNull(listener);
		if (executor == null) {
			executor = _defaultListenerExecutor;
		}
		ListenerEntry listenerEntry = new ListenerEntry(executor, _state.get().version());
		_listeners.put(new IdentityWrapper<>(listener), listenerEntry);
	}
	
	@Override
	public void addListener(TListener listener) {
		addListener(listener, null);
	}
	
	public boolean newListenerEvent(TState newState, int expectedVersion) {
		int newVersion;
		if (expectedVersion < 0) {
			newVersion = _version.getAndIncrement();
		} else {
			newVersion = expectedVersion + 1;
			if (!_version.compareAndSet(expectedVersion, newVersion)) {
				return false;
			}
		}
		VersionEntry<TState> newStateEntry = new VersionEntry<>(newState, newVersion);
		
		VersionEntry<TState> currentState = _state.get();
		boolean doUpdateAttempt = true;
		boolean callListeners = false;
		do {
			if (currentState.version() < newStateEntry.version()) {
				if (_state.compareAndSet(currentState, newStateEntry)) {
					callListeners = true;
					doUpdateAttempt = false;
				} else {
					doUpdateAttempt = true;
				}
			} else {
				doUpdateAttempt = false;
			}
		} while (doUpdateAttempt);
		
		if (callListeners) {
			for (Entry<IdentityWrapper<TListener>, ListenerEntry> entry : _listeners.entrySet()) {
				ListenerEntry listenerEntry = entry.getValue();
				callListenerIfNeeded(entry.getKey().get(), listenerEntry, newStateEntry);
			}	
		}
		return true;
	}
	
	public void newListenerEvent(final TState newState) {
		newListenerEvent(newState, -1);
	}
	
	@Override
	public void resetListenerVersion(int version, 
			final TListener listener) {
		ListenerEntry storedListenerEntry = getListenerEntry(listener);
		storedListenerEntry.updateVersion.set(version);
		callListenerIfNeeded(listener, storedListenerEntry, _state.get());
	}
	
	@Override
	public int getListenerVersion(TListener listener) {
		ListenerEntry listenerEntry = getListenerEntry(listener);
		return listenerEntry.updateVersion.get();
	}
	
	private void callListenerIfNeeded(final TListener listener, 
			ListenerEntry listenerEntry, final VersionEntry<TState> versionEntry) {
		boolean done = false;
		boolean doUpdate = false;
		do {
			// only update the listener if this state is newer than the last update
			int lastUpdateVersion = listenerEntry.updateVersion.get();
			if (lastUpdateVersion < versionEntry.version()) {
				if (listenerEntry.updateVersion.compareAndSet(lastUpdateVersion, versionEntry.version())) {
					done = true;
					doUpdate = true;
				}
			} else {
				done = true;
				doUpdate = false;
			}
		} while (!done);
		
		if (doUpdate) {
			listenerEntry.executor.execute(new Runnable() {

				@Override
				public void run() {
					TState state = versionEntry.getState();
					int updateVersion = versionEntry.version();
					_listenerInvokeDelegate.invoke(listener, state, updateVersion);
				}
				
			});
		}
	}
	
	public TState getLatestEvent() {
		return _state.get().getState();
	}
	
	private ListenerEntry getListenerEntry(TListener listener) {
		ListenerEntry storedListenerEntry = _listeners.get(new IdentityWrapper<>(listener));
		if (storedListenerEntry == null) {
			throw new IllegalArgumentException("The provided listener is not registered with this " + 
					VersioningListenableContainer.class.getSimpleName());
		}
		return storedListenerEntry;
	}
	
	public static class VersionEntry<TState> {
		
		private final TState _state;
		private final int _version;
		
		public VersionEntry(TState state, int version) {
			_state = state;
			_version = version;
		}
		
		public TState getState() {
			return _state;
		}
		
		public int version() {
			return _version;
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result
					+ ((_state == null) ? 0 : _state.hashCode());
			result = prime * result + _version;
			return result;
		}

		@Override
		public boolean equals(Object obj) {
			if (!(obj instanceof VersionEntry)) {
				return false;
			}
			VersionEntry<?> other = (VersionEntry<?>) obj;
			if (_state == null) {
				if (other._state != null) {
					return false;
				}
			} else if (!_state.equals(other._state)) {
				return false;
			}
			if (_version != other._version) {
				return false;
			}
			return true;
		}
		
	}
	
	private class ListenerEntry  {
		public final Executor executor;
		public final AtomicInteger updateVersion;
		
		public ListenerEntry(Executor executor, int version) {
			this.executor = Objects.requireNonNull(executor);
			updateVersion = new AtomicInteger(version);
		}
	}
}
