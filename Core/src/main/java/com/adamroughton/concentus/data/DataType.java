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
package com.adamroughton.concentus.data;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Objects;

import org.javatuples.Pair;
import org.javatuples.Tuple;
import org.objenesis.ObjenesisStd;

import it.unimi.dsi.fastutil.ints.Int2ObjectArrayMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;

import com.adamroughton.concentus.data.cluster.kryo.ClusterState;
import com.adamroughton.concentus.data.cluster.kryo.GuardianInit;
import com.adamroughton.concentus.data.cluster.kryo.GuardianState;
import com.adamroughton.concentus.data.cluster.kryo.MetricMetaData;
import com.adamroughton.concentus.data.cluster.kryo.MetricSourceMetaData;
import com.adamroughton.concentus.data.cluster.kryo.ProcessReturnInfo;
import com.adamroughton.concentus.data.cluster.kryo.ServiceInfo;
import com.adamroughton.concentus.data.cluster.kryo.ServiceInit;
import com.adamroughton.concentus.data.cluster.kryo.ServiceState;
import com.adamroughton.concentus.data.cluster.kryo.StateEntry;
import com.adamroughton.concentus.data.events.bufferbacked.*;
import com.adamroughton.concentus.data.model.bufferbacked.ActionReceipt;
import com.adamroughton.concentus.data.model.bufferbacked.BufferBackedEffect;
import com.adamroughton.concentus.data.model.bufferbacked.CanonicalStateUpdate;
import com.adamroughton.concentus.data.model.kryo.CandidateValue;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import static com.adamroughton.concentus.data.DataNature.*;

public enum DataType implements KryoRegistratorDelegate {
	
	NULL(0, BUFFER_BACKED, Object.class),

	/*
	 * Events
	 */
	
	MESSENGER_CONNECT_EVENT(-1, BUFFER_BACKED, ConnectEvent.class),
	CLIENT_CONNECT_EVENT(1, BUFFER_BACKED, ClientConnectEvent.class),
	CLIENT_CONNECT_RES_EVENT(2, BUFFER_BACKED, ConnectResponseEvent.class),
	CLIENT_DISCONNECTED_EVENT(3, BUFFER_BACKED, ClientDisconnectedEvent.class),
	CLIENT_INPUT_EVENT(4, BUFFER_BACKED, ClientInputEvent.class),
	CLIENT_UPDATE_EVENT(5, BUFFER_BACKED, ClientUpdateEvent.class),
	
	ACTION_EVENT(6, BUFFER_BACKED, ActionEvent.class),
	ACTION_RECEIPT_EVENT(7, BUFFER_BACKED, ActionReceiptEvent.class),
	TICK_EVENT(8, BUFFER_BACKED, TickEvent.class),
	FULL_COLLECTIVE_VAR_INPUT_EVENT(9, BUFFER_BACKED, FullCollectiveVarInputEvent.class),
	PARTIAL_COLLECTIVE_VAR_INPUT_EVENT(10, BUFFER_BACKED, PartialCollectiveVarInputEvent.class),
	
	METRIC_EVENT(10, BUFFER_BACKED, MetricEvent.class),
	
	/*
	 * Model types
	 */
	
	EFFECT(101, BUFFER_BACKED, BufferBackedEffect.class),
	CANDIDATE_VALUE(102, KRYO, CandidateValue.class),
	CANONICAL_STATE_UPDATE(103, BUFFER_BACKED, CanonicalStateUpdate.class),
	ACTION_RECEIPT(104, BUFFER_BACKED, ActionReceipt.class),
	
	/*
	 * Cluster types
	 */
	
	GUARDIAN_INIT(200, KRYO, GuardianInit.class),
	GUARDIAN_STATE(201, KRYO, GuardianState.class, new ClusterStateKryoRegistratorDelegate<>(GuardianState.class)),
	PROCESS_RETURN_INFO(202, KRYO, ProcessReturnInfo.class),
	SERVICE_STATE(203, KRYO, ServiceState.class, new ClusterStateKryoRegistratorDelegate<>(ServiceState.class)),
	STATE_ENTRY(204, KRYO, StateEntry.class),
	SERVICE_INIT(205, KRYO, ServiceInit.class),
	METRIC_META_DATA(206, KRYO, MetricMetaData.class),
	METRIC_SOURCE_META_DATA(207, KRYO, MetricSourceMetaData.class),
	SERVICE_INFO(208, KRYO, ServiceInfo.class),
	
	/*
	 * Utility types
	 */
	@SuppressWarnings("rawtypes")
	PAIR(1000, KRYO, Pair.class, new TupleKryoRegistratorDelegate<Pair>(Pair.class)),
	;	
	
	public static int nextFreeId() {
		int maxId = -1;
		for (DataType type : DataType.values()) {
			maxId = Math.max(maxId, type.getId());
		}
		return maxId + 1;
	}
	
	private final int _id;
	private final DataNature _nature;
	private final Class<?> _associatedClazz;
	private final DataTypeKryoRegistratorDelegate _kryoRegistratorDelegate;
	
	private DataType(int id, DataNature nature, Class<?> associatedClazz) {
		this(id, nature, associatedClazz, null);
	}
	
	private DataType(final int id, DataNature nature, final Class<?> associatedClazz, DataTypeKryoRegistratorDelegate registratorDelegate) {
		_id = id;
		_nature = nature;
		_associatedClazz = associatedClazz;
		if ((nature == KRYO || nature == KYRO_AND_BUFFER_BACKED) && registratorDelegate == null) {
			_kryoRegistratorDelegate = new DataTypeKryoRegistratorDelegate() {
				
				@Override
				public void register(int id, Kryo kryo) {
					kryo.register(associatedClazz, id);
				}
			};
		} else {
			_kryoRegistratorDelegate = registratorDelegate;			
		}
	}
	
	public int getId() {
		return _id;
	}
	
	public DataNature getNature() {
		return _nature;
	}
	
	public Class<?> getEventClass() {
		return _associatedClazz;
	}

	@Override
	public void register(Kryo kryo) {
		if (_kryoRegistratorDelegate != null) {
			_kryoRegistratorDelegate.register(_id, kryo);
		}
	}
	
	/*
	 * Helper classes
	 */
	
	private static interface DataTypeKryoRegistratorDelegate {
		void register(int id, Kryo kryo);
	}
	
	private static class ClusterStateKryoRegistratorDelegate<TState extends Enum<TState> & ClusterState> 
			implements DataTypeKryoRegistratorDelegate {

		private final Class<TState> _type;
		private final Int2ObjectMap<TState> _clusterStateLookup;
		
		public ClusterStateKryoRegistratorDelegate(Class<TState> type) {
			_type = Objects.requireNonNull(type);
			TState[] values = type.getEnumConstants();
			_clusterStateLookup = new Int2ObjectArrayMap<>(values.length);
			for (TState value : values) {
				_clusterStateLookup.put(value.code(), value);
			}
		}
		
		@Override
		public void register(int id, Kryo kryo) {
			kryo.register(_type, new Serializer<TState>(false) {

				@Override
				public void write(Kryo kryo, Output output, TState state) {
					// nulls are handled by kryo
					output.writeInt(state.code());
				}

				@Override
				public TState read(Kryo kryo, Input input,
						Class<TState> type) {
					// nulls are handled by kryo
					int stateCode = input.readInt();
					return _clusterStateLookup.get(stateCode);
				}
			}, id);			
		}
		
	}
	
	public static interface TupleCreateDelegate<TTuple extends Tuple> {
		TTuple newInstance(Object[] array);
	}
	
	private static class TupleSerializer<TTuple extends Tuple> extends Serializer<TTuple> {
		
		private final Class<TTuple> _tupleType;
		private final Field _valuesArrayField;
		private final Field _valuesListField;
		private final Field[] _valueFields;
		private final ObjenesisStd _objenesis;
		
		public TupleSerializer(Class<TTuple> tupleType) {
			try {
				_tupleType = Objects.requireNonNull(tupleType);
				
				_valuesArrayField = Tuple.class.getDeclaredField("valueArray");
				_valuesArrayField.setAccessible(true);
				
				_valuesListField = Tuple.class.getDeclaredField("valueList");
				_valuesListField.setAccessible(true);
				
				_objenesis = new ObjenesisStd();
				
				Field sizeField = tupleType.getDeclaredField("SIZE");
				sizeField.setAccessible(true);
				int tupleLength = sizeField.getInt(null);
				
				_valueFields = new Field[tupleLength];
				for (int i = 0; i < tupleLength; i++) {
					_valueFields[i] = tupleType.getDeclaredField("val" + i);
					_valueFields[i].setAccessible(true);
				}
	        } catch (Exception e) {
	            throw new RuntimeException(e);
	        }	
		}
		
		@Override
		public void write(Kryo kryo, Output output, TTuple tuple) {
			kryo.writeObject(output, tuple.toArray());
		}

		@SuppressWarnings("unchecked")
		@Override
		public TTuple read(Kryo kryo, Input input, Class<TTuple> type) {
			try {
				if (!type.equals(_tupleType)) {
					throw new RuntimeException("The given tuple type: " + type.getName() + 
							" does not match the tuple type of this serializer: " + _tupleType.getName());
				}
				
				/*
				 * TODO: Pretty hacky - relies on children not accessing members
				 * on the object during their deserialization.
				 */
				Object tuple = _objenesis.newInstance(type);
				kryo.reference(tuple);
			
				Object[] values = kryo.readObject(input, Object[].class);
				_valuesArrayField.set(tuple, values);
				
				_valuesListField.set(tuple, Arrays.asList(values));
				
				for (int i = 0; i < values.length; i++) {
					_valueFields[i].set(tuple, values[i]);
				}
				
				return (TTuple) tuple;
			} catch (Exception e) {
				throw new RuntimeException(e);
			}			
		}
		
	}
	
	private static class TupleKryoRegistratorDelegate<TTuple extends Tuple> implements DataTypeKryoRegistratorDelegate {
		
		private final Class<TTuple> _tupleType;
		
		public TupleKryoRegistratorDelegate(Class<TTuple> tupleType) {
			_tupleType = Objects.requireNonNull(tupleType);
		}
		
		@Override
		public void register(int id, Kryo kryo) {
			kryo.register(_tupleType, new TupleSerializer<>(_tupleType), id);
		}
		
	}

}
