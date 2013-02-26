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
package com.adamroughton.consentus.messaging;

import java.util.Arrays;
import java.util.Objects;

public final class MultiSocketSettings {
	
	// use positive indices for stdSockets, negative indices for 
	// subSockets (which map to positive indices in the subSocket array)
	// indices start from 1 to avoid ambiguity on 0 
	// (i.e 1 in order => 0 in stdSocket, -1 in order => 0 in subSocket)
	private final int[] _order; 
	private final SocketSettings[] _stdSockets; 
	private final SubSocketSettings[] _subSockets;
		
	public static MultiSocketSettings beginWith(final SocketSettings socketSetting) {
		Objects.requireNonNull(socketSetting);
		
		if (SubSocketSettings.isSubSocket(socketSetting.getSocketType())) {
			return beginWith(SubSocketSettings.wrapImplicit(socketSetting));
		}
		
		return new MultiSocketSettings(new int[] { 1 }, 
				new SocketSettings[] { socketSetting }, 
				new SubSocketSettings[0]);
	}
	
	public static MultiSocketSettings beginWith(final SubSocketSettings subSocketSetting) {
		Objects.requireNonNull(subSocketSetting);
		return new MultiSocketSettings(new int[] { -1 }, 
				new SocketSettings[0], 
				new SubSocketSettings[] { subSocketSetting });
	}
	
	private MultiSocketSettings(final int[] order, 
			final SocketSettings[] stdSockets,
			final SubSocketSettings[] subSockets) {
		_order = order;
		_stdSockets = stdSockets;
		_subSockets = subSockets;
	}
	
	public MultiSocketSettings then(final SocketSettings socketSetting) {
		Objects.requireNonNull(socketSetting);
		
		if (SubSocketSettings.isSubSocket(socketSetting.getSocketType())) {
			return then(SubSocketSettings.wrapImplicit(socketSetting));
		}
		
		int[] order = new int[_order.length + 1];
		System.arraycopy(_order, 0, order, 0, _order.length);
		order[_order.length] = _stdSockets.length + 1;
		
		SocketSettings[] stdSockets = new SocketSettings[_stdSockets.length + 1];
		System.arraycopy(_stdSockets, 0, stdSockets, 0, _stdSockets.length);
		stdSockets[_stdSockets.length] = socketSetting;
		
		SubSocketSettings[] subSockets = Arrays.copyOf(_subSockets, _subSockets.length);		
		return new MultiSocketSettings(order, stdSockets, subSockets);
	}
	
	public MultiSocketSettings then(final SubSocketSettings subSocketSetting) {
		Objects.requireNonNull(subSocketSetting);
		
		int[] order = new int[_order.length + 1];
		System.arraycopy(_order, 0, order, 0, _order.length);
		order[_order.length] = -(_subSockets.length + 1);
		
		SubSocketSettings[] subSockets = new SubSocketSettings[_subSockets.length + 1];
		System.arraycopy(_subSockets, 0, subSockets, 0, _subSockets.length);
		subSockets[_subSockets.length] = subSocketSetting;
		
		SocketSettings[] stdSockets = Arrays.copyOf(_stdSockets, _stdSockets.length);		
		return new MultiSocketSettings(order, stdSockets, subSockets);
	}
	
	public boolean isSub(int orderIndex) {
		return _order[orderIndex] < 0;
	}
	
	public int socketCount() {
		return _order.length;
	}
	
	public SubSocketSettings getSubSocketSettings(int orderIndex) {
		if (!isSub(orderIndex)) throw new RuntimeException(String.format("No sub socket settings at index %d", orderIndex));
		return _subSockets[-_order[orderIndex] - 1]; // order indices begin at 1
	}
	
	public SocketSettings getSocketSettings(int orderIndex) {
		if (isSub(orderIndex)) {
			return getSubSocketSettings(orderIndex).getSocketSettings();
		} else {
			return _stdSockets[_order[orderIndex] - 1]; // order indices begin at 1
		}
	}
}
