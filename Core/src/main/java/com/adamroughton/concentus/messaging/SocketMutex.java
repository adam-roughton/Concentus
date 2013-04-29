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
package com.adamroughton.concentus.messaging;

import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.adamroughton.concentus.util.Mutex;

public final class SocketMutex implements Mutex<SocketPackage> {
	
	private final SocketPackage _socketPackage;
	
	private final Lock _lock = new ReentrantLock();
	private final Condition _condition = _lock.newCondition();
	private Thread _owningThread = null;
	private int _reentrantCount = 0;
	
	public SocketMutex(SocketPackage socketPackage) {
		_socketPackage = Objects.requireNonNull(socketPackage);
	}
	
	/**
	 * Executes the provided runnable object with exclusive
	 * access to the socket, releasing access when the runnable
	 * completes.
	 * 
	 * @param delegate the delegate to execute
	 */
	public void runAsOwner(OwnerDelegate<SocketPackage> delegate) {
		try {
			if (!takeOwnership()) {
				throw new IllegalStateException(String.format("The socket is already in use by thread %s", 
						Thread.currentThread().getName()));
			}
			delegate.asOwner(_socketPackage);
		} finally {
			releaseOwnership();
		}
	}
	
	public boolean isOwned() {
		_lock.lock();
		try {
			return _owningThread != null;
		} finally {
			_lock.unlock();
		}		
	}
	
	public void waitForRelease() throws InterruptedException {
		_lock.lock();
		try {
			while(_owningThread != null) {
				_condition.await();
			}
		} finally {
			_lock.unlock();
		}
	}
	
	public void waitForRelease(long timeout, TimeUnit unit) throws InterruptedException {
		long startTime = System.nanoTime();
		_lock.lock();
		try {
			while(_owningThread != null && System.nanoTime() - startTime < unit.toNanos(timeout)) {
				_condition.await(timeout, unit);
			}
		} finally {
			_lock.unlock();
		}
	}
	
	private boolean takeOwnership() {
		Thread current = Thread.currentThread();
		_lock.lock();
		try {
			if (_owningThread == null) {
				_owningThread = current;
				_reentrantCount = 0;
			} else if (_owningThread == current) {
				_reentrantCount++;
			} else {
				return false;
			}
			_condition.signalAll();
			return true;
		} finally {
			_lock.unlock();
		}
	}
	
	private void releaseOwnership() {
		Thread current = Thread.currentThread();
		_lock.lock();
		try {
			if (_owningThread == current) {
				if (_reentrantCount > 0) {
					_reentrantCount--;
				} else {
					_owningThread = null;
				}
			}
			_condition.signalAll();
		} finally {
			_lock.unlock();
		}
	}
	
	public static <TSocketSet extends SocketSet> SocketSetMutex<TSocketSet> createSetMutex(
			SocketSetFactory<TSocketSet> factory, 
			SocketMutex first, SocketMutex...additional) {
		return new SocketSetMutex<TSocketSet>(factory, first, additional);
	}
	
	public interface SocketSetFactory<TSocketSet extends SocketSet> {
		TSocketSet create(SocketPackage...packages);
	}
	
	public static class SocketSetMutex<TSocketSet extends SocketSet> implements Mutex<TSocketSet> {
		private final SocketSetFactory<TSocketSet> _factory;
		private final SocketMutex[] _tokens;
		
		public SocketSetMutex(SocketSetFactory<TSocketSet> factory, SocketMutex first, SocketMutex...additional) {
			_factory = Objects.requireNonNull(factory);
			_tokens = new SocketMutex[additional.length + 1];
			_tokens[0] = Objects.requireNonNull(first);
			System.arraycopy(additional, 0, _tokens, 1, additional.length);
		}
		
		public void runAsOwner(OwnerDelegate<TSocketSet> delegate) {
			final SocketPackage[] packages = new SocketPackage[_tokens.length];
			boolean[] ownedSockets = new boolean[_tokens.length];
			try {
				for (int i = 0; i < _tokens.length; i++) {
					_tokens[i].takeOwnership();
					ownedSockets[i] = true;
					packages[i] = _tokens[i]._socketPackage;
				}
				delegate.asOwner(_factory.create(packages));
			} finally {
				for (int i = 0; i < _tokens.length; i++) {
					if (ownedSockets[i]) {
						_tokens[i].releaseOwnership();
					}
				}
			}
		}

		@Override
		public boolean isOwned() {
			boolean[] lockedSockets = new boolean[_tokens.length];
			try {
				boolean isOwned = false;
				for (int i = 0; i < _tokens.length; i++) {
					_tokens[i]._lock.lock();
					lockedSockets[i] = true;
					isOwned |= _tokens[i].isOwned();
					if (isOwned) break;
				}
				return isOwned;
			} finally {
				for (int i = 0; i < _tokens.length; i++) {
					if (lockedSockets[i]) {
						_tokens[i]._lock.unlock();
					}
				}
			}
		}

		@Override
		public void waitForRelease() throws InterruptedException {
			for (Mutex<?> socketMutex : _tokens) {
				socketMutex.waitForRelease();
			}
		}

		@Override
		public void waitForRelease(long timeout, TimeUnit unit)
				throws InterruptedException {
			long startTime = System.nanoTime();
			long remainingTime;
			for (Mutex<?> socketMutex : _tokens) {
				remainingTime = unit.toNanos(timeout) - System.nanoTime() - startTime;
				remainingTime = Math.min(remainingTime, 0);
				if (remainingTime > 0) {
					socketMutex.waitForRelease(remainingTime, TimeUnit.NANOSECONDS);
				} else {
					return;
				}
			}
		}
	}
	
}
