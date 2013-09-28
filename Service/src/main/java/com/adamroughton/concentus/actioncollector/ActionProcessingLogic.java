package com.adamroughton.concentus.actioncollector;

import it.unimi.dsi.fastutil.ints.Int2ObjectArrayMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.objects.Object2ObjectMap;
import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectIterator;

import java.util.HashSet;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Set;

import com.adamroughton.concentus.data.ArrayBackedResizingBuffer;
import com.adamroughton.concentus.data.NullResizingBuffer;
import com.adamroughton.concentus.data.ResizingBuffer;
import com.adamroughton.concentus.data.model.ClientId;
import com.adamroughton.concentus.data.model.Effect;
import com.adamroughton.concentus.data.model.bufferbacked.BufferBackedEffect;
import com.adamroughton.concentus.data.model.kryo.CandidateValue;
import com.adamroughton.concentus.model.CollectiveApplication;
import com.adamroughton.concentus.model.UserEffectSet;
import com.adamroughton.concentus.util.Util;

public final class ActionProcessingLogic {
	
	private final CollectiveApplication _application;
	private final long _tickDuration;
	private final Object2ObjectMap<ClientVarKey, ArrayBackedResizingBuffer> _effects;
	
	private final BufferBackedEffect _effectWrapper = new BufferBackedEffect();
	
	private long _lastTick;
	
	public ActionProcessingLogic(CollectiveApplication application, long tickDuration, long startTime) {
		_application = application;
		_effects = new Object2ObjectOpenHashMap<>();
		_tickDuration = tickDuration;
		_lastTick = startTime;
	}
	
	public long nextTickTime() {
		return _lastTick + _tickDuration;
	}
	
	public Iterator<ResizingBuffer> newAction(final long clientId, int actionTypeId, ResizingBuffer actionData) {
		/*
		 * Always use the latest effect for a given variable ID
		 */
		final Int2ObjectMap<ResizingBuffer> updatedEffectsMap = new Int2ObjectArrayMap<>();
		final Set<BufferBackedEffect> _createdWrappers = new HashSet<>();
		_application.processAction(new UserEffectSet() {
			
			@Override
			public ClientId getClientId() {
				return ClientId.fromBits(clientId);
			}

			@Override
			public long getClientIdBits() {
				return clientId;
			}

			@Override
			public boolean hasEffectFor(int variableId) {
				return _effects.containsKey(new ClientVarKey(clientId, variableId));
			}

			@Override
			public boolean cancelEffect(int variableId) {
				ClientVarKey key = new ClientVarKey(clientId, variableId);
				if (_effects.containsKey(key)) {
					_effectWrapper.attachToBuffer(_effects.get(key));
					_effectWrapper.setIsCancelled(true);
					updatedEffectsMap.put(variableId, _effectWrapper.getBuffer());
					_effects.remove(key);
					_effectWrapper.releaseBuffer();
					return true;
				} else {
					return false;
				}
			}

			@Override
			public Effect getEffect(int variableId) {
				ClientVarKey key = new ClientVarKey(clientId, variableId);
				if (_effects.containsKey(key)) {
					BufferBackedEffect effect = new BufferBackedEffect();
					_createdWrappers.add(effect);
					effect.attachToBuffer(_effects.get(key));
					return effect;
				} else {
					return null;
				}
			}

			@Override
			public boolean newEffect(int variableId, int effectTypeId,
					byte[] effectData) {
				return newEffect(variableId, effectTypeId, effectData, 
						0, effectData.length);
			}

			@Override
			public boolean newEffect(int variableId, int effectTypeId,
					byte[] effectData, int offset, int length) {
				BufferBackedEffect effect = newEffect(variableId, effectTypeId, length);
				effect.getDataSlice().copyFrom(effectData, offset, 0, length);
				return true;
			}

			@Override
			public boolean newEffect(int variableId, int effectTypeId,
					ResizingBuffer effectData) {
				return newEffect(variableId, effectTypeId, effectData, 
						0, effectData.getContentSize());
			}

			@Override
			public boolean newEffect(int variableId, int effectTypeId,
					ResizingBuffer effectData, int offset, int length) {
				BufferBackedEffect effect = newEffect(variableId, effectTypeId, length);
				effectData.copyTo(effect.getDataSlice(), offset, 0, length);
				return true;
			}
			
			private BufferBackedEffect newEffect(int variableId, int effectTypeId, int dataLength) {
				ClientVarKey key = new ClientVarKey(clientId, variableId);
				ArrayBackedResizingBuffer effectBuffer = new ArrayBackedResizingBuffer(Util.nextPowerOf2(32 + dataLength));
				_effects.put(key, effectBuffer);
				
				_effectWrapper.attachToBuffer(effectBuffer);
				_effectWrapper.writeTypeId();
				_effectWrapper.setClientIdBits(clientId);
				_effectWrapper.setEffectTypeId(effectTypeId);
				_effectWrapper.setVariableId(variableId);
				_effectWrapper.setIsCancelled(false);
				_effectWrapper.setStartTime(nextTickTime());
				updatedEffectsMap.put(variableId, effectBuffer);
				return _effectWrapper;
			}
			
		}, actionTypeId, actionData);
		
		// tidy up
		for (BufferBackedEffect effect : _createdWrappers) {
			effect.releaseBuffer();
		}
		
		return updatedEffectsMap.values().iterator();
	}
	
	public ResizingBuffer getEffectData(long clientIdBits, int varId) {
		ClientVarKey clientVarKey = new ClientVarKey(clientIdBits, varId);
		ResizingBuffer effectData = _effects.get(clientVarKey);
		if (effectData == null) {
			effectData = new NullResizingBuffer();
		}
		return effectData;
	}
	
	public Iterator<CandidateValue> tick(final long time) {
		_lastTick = time;
		
		final ObjectIterator<ArrayBackedResizingBuffer> iterator = _effects.values().iterator();
		return new Iterator<CandidateValue>() {

			CandidateValue _next = null;
			BufferBackedEffect _effect = new BufferBackedEffect();
			
			@Override
			public boolean hasNext() {
				if (_next == null) {
					while (iterator.hasNext()) {
						ArrayBackedResizingBuffer next = iterator.next();
						_effect.attachToBuffer(next);
						_next = _application.apply(_effect, time);
						_effect.releaseBuffer();
						if (_next.getScore() > 0) {
							return true;
						} else if (_next.getScore() < 0) {
							iterator.remove();
						}
					}
					return false;
				} else {
					return true;
				}
			}

			@Override
			public CandidateValue next() {
				if (_next == null)
					throw new NoSuchElementException();
				CandidateValue next = _next;
				_next = null;
				return next;
			}

			@Override
			public void remove() {
				throw new UnsupportedOperationException();
			}
			
		};	
	}
	
	private static class ClientVarKey {
		
		public final long clientIdBits;
		public final int variableId;
		
		public ClientVarKey(long clientIdBits, int variableId) {
			this.clientIdBits = clientIdBits;
			this.variableId = variableId;
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result
					+ (int) (clientIdBits ^ (clientIdBits >>> 32));
			result = prime * result + variableId;
			return result;
		}

		@Override
		public boolean equals(Object obj) {
			if (!(obj instanceof ClientVarKey))
				return false;
			ClientVarKey other = (ClientVarKey) obj;
			if (this.clientIdBits != other.clientIdBits)
				return false;
			if (this.variableId != other.variableId)
				return false;
			return true;
		}
		
	}
	
}
