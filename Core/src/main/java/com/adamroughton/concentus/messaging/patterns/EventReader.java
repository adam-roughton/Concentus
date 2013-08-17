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
package com.adamroughton.concentus.messaging.patterns;

import com.adamroughton.concentus.messaging.IncomingEventHeader;
import com.adamroughton.concentus.messaging.events.BufferBackedObject;

public interface EventReader<TRecvHeader extends IncomingEventHeader, TEvent extends BufferBackedObject> {

	/**
	 * Reads the content of an event using the given
	 * {@link BufferBackedObject} instance.
	 * @param header the header of the event
	 * @param event the event to read from
	 */
	void read(TRecvHeader header, TEvent event);
	
}
