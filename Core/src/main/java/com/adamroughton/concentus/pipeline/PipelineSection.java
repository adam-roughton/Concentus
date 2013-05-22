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
package com.adamroughton.concentus.pipeline;

import java.util.ArrayList;
import java.util.Collection;

import com.adamroughton.concentus.Clock;
import com.adamroughton.concentus.disruptor.EventQueue;
import com.adamroughton.concentus.pipeline.ProcessingPipeline.PipelineSectionJoin;

public class PipelineSection<TEvent> {

	private final int _layerIndex;
	private final EventQueue<TEvent> _sectionConnector;
	private final PipelineTopology<TEvent> _pipelineSection;
	private final Clock _clock;
	
	PipelineSection(int layerIndex, EventQueue<TEvent> connector, PipelineTopology<TEvent> pipelineSection, Clock clock) {
		_layerIndex = layerIndex;
		_sectionConnector = connector;
		_pipelineSection = pipelineSection;
		_clock = clock;
	}
	
	@SafeVarargs
	public final PipelineSectionJoinBase<TEvent> join(PipelineSection<TEvent>...sections) {
		ArrayList<PipelineSection<TEvent>> sectionsList = new ArrayList<>();
		for (PipelineSection<TEvent> section : sections) {
			sectionsList.add(section);
		}
		return new PipelineSectionJoin<>(sectionsList, _clock);
	}
	
	public static abstract class PipelineSectionJoinBase<TEvent> {
		
		protected final int _layerIndex;
		protected final Collection<EventQueue<TEvent>> _connectors;
		protected final PipelineTopology<TEvent> _pipeline;
		protected final Clock _clock;
		
		PipelineSectionJoinBase(Collection<PipelineSection<TEvent>> sections, Clock clock) {
			_connectors = new ArrayList<>(sections.size());
			_clock = clock;
			
			// work out the largest pipeline section so that we can join
			// the sections at the same place
			
			int highestLayerIndex = 0;
			PipelineTopology<TEvent> pipelineWithHighestLayerIndex = new PipelineTopology<>();
			for (PipelineSection<TEvent> section : sections) {
				if (section._layerIndex > highestLayerIndex) {
					highestLayerIndex = section._layerIndex;
					pipelineWithHighestLayerIndex = section._pipelineSection;
				}
			}
			for (PipelineSection<TEvent> section : sections) {
				_connectors.add(section._sectionConnector);
				if (section._pipelineSection != pipelineWithHighestLayerIndex) {
					pipelineWithHighestLayerIndex = pipelineWithHighestLayerIndex.combineWith(section._pipelineSection, highestLayerIndex - section._layerIndex);
				}
			}
			_pipeline = pipelineWithHighestLayerIndex;
			_layerIndex = highestLayerIndex;
		}
		
		protected PipelineSegment<TEvent> createSegment(ConsumingPipelineProcess<TEvent> process) {
			PipelineSegment<TEvent> segment = new PipelineSegment<>(_connectors, process, _clock);
			for (EventQueue<TEvent> connector : _connectors) {
				connector.setGatingSequences(process.getSequence());
			}
			return segment;
		}
		
		protected PipelineTopology<TEvent> intoInternal(ConsumingPipelineProcess<TEvent> process) {
			PipelineSegment<TEvent> segment = createSegment(process);
			return _pipeline.add(_layerIndex, segment);
		}
		
	}
	
}
