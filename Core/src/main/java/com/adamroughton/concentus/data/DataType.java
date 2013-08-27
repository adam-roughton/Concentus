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

import com.adamroughton.concentus.data.events.bufferbacked.*;
import com.adamroughton.concentus.data.model.bufferbacked.ActionReceipt;
import com.adamroughton.concentus.data.model.bufferbacked.BufferBackedEffect;
import com.adamroughton.concentus.data.model.bufferbacked.CanonicalStateUpdate;
import com.adamroughton.concentus.data.model.kyro.CandidateValue;
import com.esotericsoftware.kryo.Kryo;

import static com.adamroughton.concentus.data.DataNature.*;

public enum DataType implements KryoRegistratorDelegate {
	
	NULL(0, KYRO_AND_BUFFER_BACKED, Object.class),

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
	METRIC_META_DATA_EVENT(11, BUFFER_BACKED, MetricMetaDataEvent.class),
	METRIC_META_DATA_REQ_EVENT(12, BUFFER_BACKED, MetricMetaDataRequestEvent.class),
	
	/*
	 * Model types
	 */
	
	EFFECT(101, BUFFER_BACKED, BufferBackedEffect.class),
	CANDIDATE_VALUE(102, KRYO, CandidateValue.class),
	CANONICAL_STATE_UPDATE(103, BUFFER_BACKED, CanonicalStateUpdate.class),
	ACTION_RECEIPT(104, BUFFER_BACKED, ActionReceipt.class)
	
	
	;	
	private final int _id;
	private final DataNature _nature;
	private final Class<?> _associatedClazz;
	private final KryoRegistratorDelegate _kryoRegistratorDelegate;
	
	private DataType(int id, DataNature nature, Class<?> associatedClazz) {
		this(id, nature, associatedClazz, null);
	}
	
	private DataType(final int id, DataNature nature, final Class<?> associatedClazz, KryoRegistratorDelegate registratorDelegate) {
		_id = id;
		_nature = nature;
		_associatedClazz = associatedClazz;
		if ((nature == KRYO || nature == KYRO_AND_BUFFER_BACKED) && registratorDelegate == null) {
			_kryoRegistratorDelegate = new KryoRegistratorDelegate() {
				
				@Override
				public void register(Kryo kryo) {
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
			_kryoRegistratorDelegate.register(kryo);
		}
	}
}
