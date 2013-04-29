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
import com.adamroughton.concentus.messaging.MessagingUtil.SocketDependentEventHandler;

public class Publisher implements SocketDependentEventHandler<byte[], SocketPackage> {

	private final OutgoingEventHeader _header;
	
	public Publisher(final OutgoingEventHeader header) {
		_header = Objects.requireNonNull(header);
	}

	@Override
	public void onEvent(byte[] event, long sequence, boolean endOfBatch, SocketPackage socketPackage)
			throws Exception {
		Messaging.send(socketPackage, event, _header, true);
	}

}


