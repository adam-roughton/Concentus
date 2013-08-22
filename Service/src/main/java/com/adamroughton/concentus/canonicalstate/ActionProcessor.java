package com.adamroughton.concentus.canonicalstate;

import it.unimi.dsi.fastutil.ints.Int2ObjectArrayMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.objects.Object2ObjectMap;
import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectIterator;

import java.util.Iterator;
import java.util.NoSuchElementException;

import com.adamroughton.concentus.messaging.ResizingBuffer;
import com.adamroughton.concentus.messaging.events.data.EffectData;
import com.adamroughton.concentus.model.CandidateValue;
import com.adamroughton.concentus.model.ClientId;
import com.adamroughton.concentus.model.CollectiveApplication;
import com.adamroughton.concentus.model.Effect;
import com.adamroughton.concentus.model.UserEffectSet;

public final class ActionProcessor {

	private final CollectiveApplication _application;
	private final long _tickDuration;
	private final Object2ObjectMap<ClientVarKey, EffectData> _effects;
	
	private long _lastTick;
	
	public ActionProcessor(CollectiveApplication application, long tickDuration, long startTime) {
		_application = application;
		_effects = new Object2ObjectOpenHashMap<>();
		_tickDuration = tickDuration;
		_lastTick = startTime;
	}
	
	public Iterator<EffectData> newAction(final long clientId, int actionTypeId, ResizingBuffer actionData) {
		/*
		 * Always use the latest effect for a given variable ID
		 */
		final Int2ObjectMap<EffectData> updatedEffectsMap = new Int2ObjectArrayMap<>();
		
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
					EffectData effect = _effects.get(key);
					effect.setIsCancelled(true);
					updatedEffectsMap.put(variableId, effect);
					_effects.remove(key);
					return true;
				} else {
					return false;
				}
			}

			@Override
			public Effect getEffect(int variableId) {
				return _effects.get(new ClientVarKey(clientId, variableId));
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
				EffectData effect = newEffect(variableId, effectTypeId);
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
				EffectData effect = newEffect(variableId, effectTypeId);
				effectData.copyTo(effect.getDataSlice(), offset, 0, length);
				return true;
			}
			
			private EffectData newEffect(int variableId, int effectTypeId) {
				EffectData effect = new EffectData();
				effect.setClientIdBits(clientId);
				effect.setEffectTypeId(effectTypeId);
				effect.setVariableId(variableId);
				effect.setIsCancelled(false);
				effect.setStartTime(_lastTick + _tickDuration);
				updatedEffectsMap.put(variableId, effect);
				return effect;
			}
			
		}, actionTypeId, actionData);
		
		return updatedEffectsMap.values().iterator();
	}
	
	public Iterator<CandidateValue> tick(final long time) {
		final ObjectIterator<EffectData> iterator = _effects.values().iterator();
		return new Iterator<CandidateValue>() {

			CandidateValue _next = null;
			
			@Override
			public boolean hasNext() {
				while (iterator.hasNext()) {
					EffectData next = iterator.next();
					_next = _application.apply(next, time);
					if (_next.getScore() > 0) {
						return true;
					} else if (_next.getScore() < 0) {
						iterator.remove();
					}
				}
				return false;
			}

			@Override
			public CandidateValue next() {
				if (_next == null)
					throw new NoSuchElementException();
				return _next;
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
