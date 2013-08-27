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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.adamroughton.concentus.data.ResizingBuffer;
import com.adamroughton.concentus.util.Mutex;

public final class MessengerMutex<TBuffer extends ResizingBuffer, TMessenger extends Messenger<TBuffer>> implements Mutex<Messenger<TBuffer>> {
	
	private final TMessenger _messenger;
	
	private final Lock _lock = new ReentrantLock();
	private final Condition _condition = _lock.newCondition();
	private Thread _owningThread = null;
	private int _reentrantCount = 0;
	
	public MessengerMutex(TMessenger messenger) {
		_messenger = Objects.requireNonNull(messenger);
	}
	
	/**
	 * Executes the provided runnable object with exclusive
	 * access to the messenger, releasing access when the runnable
	 * completes.
	 * 
	 * @param delegate the delegate to execute
	 */
	public void runAsOwner(final OwnerDelegate<Messenger<TBuffer>> delegate) {
		directRunAsOwner(new OwnerDelegate<TMessenger>() {

			@Override
			public void asOwner(TMessenger messenger) {
				delegate.asOwner(messenger);
			}
		});
	}
	
	public void directRunAsOwner(OwnerDelegate<TMessenger> delegate) {
		try {
			if (!takeOwnership()) {
				throw new IllegalStateException(String.format("The messenger is already in use by thread %s", 
						Thread.currentThread().getName()));
			}
			delegate.asOwner(_messenger);
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
	
	public static <TBuffer extends ResizingBuffer, 
				TSetMessenger extends Messenger<TBuffer>,
				TBaseMessenger extends Messenger<TBuffer>> 
		MultiMessengerMutex<TBuffer, TSetMessenger, TBaseMessenger> createMultiMessengerMutex(
			MultiMessengerFactory<TBuffer, TSetMessenger, TBaseMessenger> factory, 
			Collection<MessengerMutex<TBuffer, TBaseMessenger>> messengerMutexes) {
		return new MultiMessengerMutex<>(factory, messengerMutexes);
	}
	
	public interface MultiMessengerFactory<TBuffer extends ResizingBuffer, TSetMessenger extends Messenger<TBuffer>, TBaseMessenger extends Messenger<TBuffer>> {
		TSetMessenger create(Collection<TBaseMessenger> messengers);
	}
	
	public static class MultiMessengerMutex<TBuffer extends ResizingBuffer, 
			TSetMessenger extends Messenger<TBuffer>,
			TBaseMessenger extends Messenger<TBuffer>> implements Mutex<TSetMessenger> {
		private final MultiMessengerFactory<TBuffer, TSetMessenger, TBaseMessenger> _factory;
		private final ArrayList<MessengerMutex<TBuffer, TBaseMessenger>> _tokens;
		
		public MultiMessengerMutex(MultiMessengerFactory<TBuffer, TSetMessenger, TBaseMessenger> factory, 
				Collection<MessengerMutex<TBuffer, TBaseMessenger>> messengerMutexes) {
			_factory = Objects.requireNonNull(factory);
			_tokens = new ArrayList<>(messengerMutexes);
		}
		
		public void runAsOwner(OwnerDelegate<TSetMessenger> delegate) {
			final ArrayList<TBaseMessenger> messengers = new ArrayList<>(_tokens.size());
			boolean[] ownedSockets = new boolean[_tokens.size()];
			try {
				for (int i = 0; i < _tokens.size(); i++) {
					_tokens.get(i).takeOwnership();
					ownedSockets[i] = true;
					messengers.add(i, _tokens.get(i)._messenger);
				}
				delegate.asOwner(_factory.create(messengers));
			} finally {
				for (int i = 0; i < _tokens.size(); i++) {
					if (ownedSockets[i]) {
						_tokens.get(i).releaseOwnership();
					}
				}
			}
		}

		@Override
		public boolean isOwned() {
			boolean[] lockedMessengers = new boolean[_tokens.size()];
			try {
				boolean isOwned = false;
				for (int i = 0; i < _tokens.size(); i++) {
					_tokens.get(i)._lock.lock();
					lockedMessengers[i] = true;
					isOwned |= _tokens.get(i).isOwned();
					if (isOwned) break;
				}
				return isOwned;
			} finally {
				for (int i = 0; i < _tokens.size(); i++) {
					if (lockedMessengers[i]) {
						_tokens.get(i)._lock.unlock();
					}
				}
			}
		}

		@Override
		public void waitForRelease() throws InterruptedException {
			for (Mutex<?> messengerMutex : _tokens) {
				messengerMutex.waitForRelease();
			}
		}

		@Override
		public void waitForRelease(long timeout, TimeUnit unit)
				throws InterruptedException {
			long startTime = System.nanoTime();
			long remainingTime;
			for (Mutex<?> messengerMutex : _tokens) {
				remainingTime = unit.toNanos(timeout) - System.nanoTime() - startTime;
				remainingTime = Math.min(remainingTime, 0);
				if (remainingTime > 0) {
					messengerMutex.waitForRelease(remainingTime, TimeUnit.NANOSECONDS);
				} else {
					return;
				}
			}
		}
	}
	
}
