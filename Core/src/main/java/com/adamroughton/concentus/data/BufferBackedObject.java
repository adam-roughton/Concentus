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


import static com.adamroughton.concentus.data.ResizingBuffer.*;

public abstract class BufferBackedObject {
	
	protected static class Field {
		public final int size;
		public int offset;
		private Field _prevField;
		private Field _nextField;
		
		/**
		 * Creates a variable length field
		 */
		public Field() {
			this(-1);
		}
		
		/**
		 * Creates a field with the given size
		 * @param size the byte length of the field
		 */
		public Field(int size) {
			this.size = size;
		}
		
		public void setNext(Field next) {
			if (_nextField != null || next._prevField != null)
				throw new IllegalStateException("A field can only be followed by one other field");
			if (size == -1) {
				throw new IllegalStateException("Variable length fields cannot be followed by another field");
			}
			_nextField = next;
			next._prevField = this;
		}
		
		public boolean isVariableLength() {
			return size == -1;
		}
		
		public Field then(int size) {
			Field newField = new Field(size);
			setNext(newField);
			return newField;
		}
		
		public Field thenVariableLength() {
			return then(-1);
		}
		
		public Field resolveOffsets() {
			if (_prevField != null) {
				_prevField.resolveOffsets();
			} else {
				int runningOffset = 0;
				for (Field field = this; field != null; field = field._nextField) {
					field.offset = runningOffset;
					if (!field.isVariableLength()) {
						runningOffset += field.size;						
					}
				}
			}
			return this;
		}
	}
	
	private final Field TYPE_ID = new Field(INT_SIZE);
	
	private final int _id;
	
	private ResizingBuffer _buffer;

	protected BufferBackedObject(int typeId) {
		_id = typeId;
	}
	
	protected BufferBackedObject(DataType dataType) {
		this(dataType.getId());
	}
	
	protected Field getBaseField() {
		return TYPE_ID;
	}
	
	public final ResizingBuffer getBuffer() {
		return _buffer;
	}
	
	public final void attachToBuffer(ResizingBuffer buffer) {
		_buffer = buffer;
	}
	
	public final void attachToBuffer(ResizingBuffer buffer, int offset) {
		_buffer = buffer.slice(offset);
	}
	
	public final int getTypeId() {
		return _id;
	}
	
	public final void writeTypeId() {
		_buffer.writeInt(0, _id);
	}
	
	public final void releaseBuffer() {
		_buffer = null;
	}

}
