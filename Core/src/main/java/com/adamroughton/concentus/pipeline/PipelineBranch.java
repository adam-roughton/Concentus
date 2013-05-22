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

import java.util.Arrays;
import java.util.Collection;
import java.util.Objects;

import com.adamroughton.concentus.Clock;
import com.lmax.disruptor.EventProcessor;
import com.lmax.disruptor.RingBuffer;

public class PipelineBranch<TEvent> {

	private final PipelineTopology<TEvent> _pipelineBranch;
	
	PipelineBranch(PipelineTopology<TEvent> pipelineBranch) {
		_pipelineBranch = pipelineBranch;
	}
	
	PipelineTopology<TEvent> getBranch() {
		return _pipelineBranch;
	}
	
	public static class Starter<TEvent> {
		
		protected final int _layerIndex;
		protected final RingBuffer<TEvent> _connector;
		protected final PipelineTopology<TEvent> _pipelineSection;
		protected final Clock _clock;
		
		Starter(RingBuffer<TEvent> startConnector, Clock clock) {
			_layerIndex = 0;
			_connector = startConnector;
			_pipelineSection = new PipelineTopology<>();
			_clock = clock;
		}
		
		Starter(int layerIndex, RingBuffer<TEvent> connector, PipelineTopology<TEvent> pipelineSection, Clock clock) {
			_layerIndex = layerIndex;
			_connector = connector;
			_pipelineSection = pipelineSection;
			_clock = clock;
		}
		
		public Builder<TEvent> then(ConsumingPipelineProcess<TEvent> process) {
			_connector.setGatingSequences(process.getSequence());
			return new Builder<>(_layerIndex + 1, process, _pipelineSection.add(_layerIndex, new PipelineSegment<>(Arrays.asList(_connector), process, _clock)), _clock);
		}
		
		public Builder<TEvent> then(EventProcessor process) {
			return then(ProcessingPipeline.<TEvent>createConsumerProcess(process, _clock));
		}
		
	}
	
	public static class Connector<TEvent> extends Starter<TEvent> {
		
		Connector(int layerIndex, RingBuffer<TEvent> connector, PipelineTopology<TEvent> pipelineSection, Clock clock) {
			super(layerIndex, connector, pipelineSection, clock);
		}
		
		public Builder<TEvent> then(EventProcessor process) {
			return then(ProcessingPipeline.<TEvent>createConsumerProcess(process, _clock));
		}
		
		@SafeVarargs
		public final PipelineSectionJoin<TEvent> join(PipelineSection<TEvent> first, PipelineSection<TEvent>...additional) {
			PipelineSection<TEvent> branchAsSection = new PipelineSection<>(_layerIndex, _connector, _pipelineSection, _clock);
			return new PipelineSectionJoin<>(ProcessingPipeline.getJoinSections(branchAsSection, first, additional), _clock);
		}
		
	}
	
	public static class Builder<TEvent> {
		
		private final int _layerIndex;
		private final ConsumingPipelineProcess<TEvent> _process;
		private final PipelineTopology<TEvent> _pipelineSection;
		private final Clock _clock;
		
		Builder(int layerIndex, ConsumingPipelineProcess<TEvent> process, PipelineTopology<TEvent> pipelineSection, Clock clock) {
			_layerIndex = layerIndex;
			_process = process;
			_pipelineSection = pipelineSection;
			_clock = clock;
		}
		
		public Connector<TEvent> thenConnector(RingBuffer<TEvent> connector) {
			Objects.requireNonNull(connector);
			connector.setGatingSequences(_process.getSequence());
			return new Connector<>(_layerIndex, connector, _pipelineSection, _clock);
		}
		
		public PipelineBranch<TEvent> create() {
			return new PipelineBranch<>(_pipelineSection);
		}
		
		@SafeVarargs
		public final PipelineBranch<TEvent> thenBranch(PipelineBranch<TEvent> firstBranch, 
				PipelineBranch<TEvent> secondBranch, 
				PipelineBranch<TEvent>...additionalBranches) {
			PipelineTopology<TEvent> pipeline = ProcessingPipeline.thenBranch(_pipelineSection, firstBranch, secondBranch, additionalBranches);
			return new PipelineBranch<TEvent>(pipeline);
		}
		
	}
	
	public static class PipelineSectionJoin<TEvent> extends PipelineSection.PipelineSectionJoinBase<TEvent> {

		PipelineSectionJoin(
				Collection<PipelineSection<TEvent>> sections,
				Clock clock) {
			super(sections, clock);
		}
		
		public Builder<TEvent> into(ConsumingPipelineProcess<TEvent> process) {
			return new Builder<>(_layerIndex + 1, process, intoInternal(process), _clock);
		}
		
		public Builder<TEvent> into(EventProcessor process) {
			return into(ProcessingPipeline.<TEvent>createConsumerProcess(process, _clock));
		}
		
	}
	
}
