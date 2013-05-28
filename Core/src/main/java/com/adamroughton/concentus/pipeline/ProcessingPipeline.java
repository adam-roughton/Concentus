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
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.ListIterator;
import java.util.Objects;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

import com.adamroughton.concentus.Clock;
import com.adamroughton.concentus.disruptor.EventQueue;
import com.adamroughton.concentus.util.Util;
import com.lmax.disruptor.EventProcessor;

public class ProcessingPipeline<TEvent> {

	private final Executor _executor;
	private final PipelineTopology<TEvent> _pipeline;
	private final Clock _clock;
	
	ProcessingPipeline(Executor executor, PipelineTopology<TEvent> pipeline, Clock clock) {
		_executor = Objects.requireNonNull(executor);
		_pipeline = Objects.requireNonNull(pipeline);
		_clock = Objects.requireNonNull(clock);
	}
	
	public void start() {
		ListIterator<Collection<PipelineSegment<TEvent>>> pipelineIterator = _pipeline.listIterator(_pipeline.size());
		while (pipelineIterator.hasPrevious()) {
			for (PipelineSegment<TEvent> segment : pipelineIterator.previous()) {
				segment.start(_executor);
			}			
		}
	}
	
	public void halt(long timeout, TimeUnit unit) throws InterruptedException {
		long startTime = _clock.nanoTime();
		long deadline = startTime + unit.toNanos(timeout);
		for (Collection<PipelineSegment<TEvent>> layer : _pipeline) {
			for (PipelineSegment<TEvent> section : layer) {
				section.halt(Util.nanosUntil(deadline, _clock), TimeUnit.NANOSECONDS);
			}
		}
	}
	
	public static <TEvent> Starter<TEvent> build(PipelineProcess<TEvent> process, Clock clock) {
		return new Starter<>(process, clock);
	}
	
	public static <TEvent> Starter<TEvent> build(Runnable process, Clock clock) {
		return build(ProcessingPipeline.<TEvent>createProducerProcess(process, clock), clock);
	}
	
	public static <TEvent> PipelineBranch.Starter<TEvent> startBranch(EventQueue<TEvent> connector, Clock clock) {
		return new PipelineBranch.Starter<>(connector, clock);
	}
	
	public static <TEvent> PipelineCyclicSection.CyclicStarter<TEvent> startCyclicPipeline(EventQueue<TEvent> connector, Clock clock) {
		return new PipelineCyclicSection.CyclicStarter<>(connector, clock);
	}
	
	public static class Starter<TEvent> {
		
		protected final PipelineTopology<TEvent> _pipelineSection;
		protected final Clock _clock;
		protected final int _layerIndex;
		
		Starter(PipelineProcess<TEvent> process, Clock clock) {
			_pipelineSection = new PipelineTopology<TEvent>().add(new PipelineSegment<>(process, clock));
			_clock = clock;
			_layerIndex = 1;
		}
		
		Starter(int layerIndex, PipelineTopology<TEvent> pipelineSection, Clock clock) {
			_pipelineSection = pipelineSection;
			_clock = clock;
			_layerIndex = layerIndex;
		}
		
		public Connector<TEvent> thenConnector(EventQueue<TEvent> connector) {
			Objects.requireNonNull(connector);
			return new Connector<>(_layerIndex, connector, _pipelineSection, _clock);
		}
		
		@SafeVarargs
		public final Creator<TEvent> thenBranch(PipelineBranch<TEvent> firstBranch, PipelineBranch<TEvent> secondBranch, PipelineBranch<TEvent>...additional) {
			PipelineTopology<TEvent> pipeline = ProcessingPipeline.thenBranch(_pipelineSection, firstBranch, secondBranch, additional);
			return new Creator<>(pipeline, _clock);
		}
		
		public final Builder<TEvent> attachBranch(PipelineBranch<TEvent> branch) {
			return attachBranches(branch);
		}
		
		@SafeVarargs
		public final Builder<TEvent> attachBranches(PipelineBranch<TEvent> firstBranch, PipelineBranch<TEvent>...additional) {
			PipelineTopology<TEvent> pipeline = ProcessingPipeline.thenBranch(_pipelineSection, firstBranch, additional);
			return new Builder<>(_layerIndex, pipeline, _clock);
		}
		
	}
	
	public static class Connector<TEvent> {
		
		private final int _layerIndex;
		private final EventQueue<TEvent> _connector;
		private final PipelineTopology<TEvent> _pipelineSection;
		private final Clock _clock;
		
		Connector(int layerIndex, EventQueue<TEvent> connector, PipelineTopology<TEvent> pipelineSection, Clock clock) {
			_layerIndex = layerIndex;
			_connector = connector;
			_pipelineSection = pipelineSection;
			_clock = clock;
		}
		
		public Builder<TEvent> then(ConsumingPipelineProcess<TEvent> process) {
			_connector.addGatingSequences(process.getSequence());
			return new Builder<>(_layerIndex + 1, _pipelineSection.add(_layerIndex, new PipelineSegment<>(Arrays.asList(_connector), process, _clock)), _clock);
		}
		
		public Builder<TEvent> then(EventProcessor process) {
			return then(ProcessingPipeline.<TEvent>createConsumerProcess(process, _clock));
		}
		
		public PipelineSection<TEvent> asSection() {
			return new PipelineSection<>(_layerIndex, _connector, _pipelineSection, _clock);
		}
		
		@SafeVarargs
		public final PipelineSectionJoin<TEvent> join(PipelineSection<TEvent> first, PipelineSection<TEvent>...additional) {
			return new PipelineSectionJoin<>(ProcessingPipeline.getJoinSections(this.asSection(), first, additional), _clock);
		}
		
	}
	
	public static class Builder<TEvent> extends Starter<TEvent> {

		Builder(int layerIndex, PipelineTopology<TEvent> pipelineSection, Clock clock) {
			super(layerIndex, pipelineSection, clock);
		}
		
		public ProcessingPipeline<TEvent> createPipeline(Executor executor) {
			return new ProcessingPipeline<>(executor, _pipelineSection, _clock);
		}
		
	}
	
	public static class Creator<TEvent> {
		
		private final PipelineTopology<TEvent> _pipelineSection;
		private final Clock _clock;
		
		Creator(PipelineTopology<TEvent> pipelineSection, Clock clock) {
			_pipelineSection = pipelineSection;
			_clock = clock;
		}
		
		public ProcessingPipeline<TEvent> createPipeline(Executor executor) {
			return new ProcessingPipeline<>(executor, _pipelineSection, _clock);
		}
		
	}
	
	public static class PipelineSectionJoin<TEvent> extends PipelineSection.PipelineSectionJoinBase<TEvent> {

		PipelineSectionJoin(
				Collection<PipelineSection<TEvent>> sections,
				Clock clock) {
			super(sections, clock);
		}
		
		public Builder<TEvent> into(ConsumingPipelineProcess<TEvent> process) {
			return new Builder<>(_layerIndex + 1, intoInternal(process), _clock);
		}
		
		public Builder<TEvent> into(EventProcessor process) {
			return into(ProcessingPipeline.<TEvent>createConsumerProcess(process, _clock));
		}
		
	}
	
	@SafeVarargs
	static <TEvent> Collection<PipelineSection<TEvent>> getJoinSections(PipelineSection<TEvent> baseSection, PipelineSection<TEvent> first, PipelineSection<TEvent>...additional) {
		Objects.requireNonNull(baseSection);
		Objects.requireNonNull(first);
		
		ArrayList<PipelineSection<TEvent>> sections = new ArrayList<>();
		sections.add(baseSection);
		sections.add(first);
		for (PipelineSection<TEvent> section : additional) {
			sections.add(section);
		}
		return sections;
	}
	
	static <TEvent> PipelineTopology<TEvent> thenBranch(
			PipelineTopology<TEvent> pipelineSection,
			PipelineBranch<TEvent> firstBranch,  
			@SuppressWarnings("unchecked") PipelineBranch<TEvent>...additionalBranches) {
		Objects.requireNonNull(firstBranch);
		
		List<PipelineBranch<TEvent>> branches = new ArrayList<>(Arrays.asList(additionalBranches));
		branches.add(0, firstBranch);
		
		return thenBranch(pipelineSection, branches);
	}
	
	static <TEvent> PipelineTopology<TEvent> thenBranch(
			PipelineTopology<TEvent> pipelineSection,
			PipelineBranch<TEvent> firstBranch, 
			PipelineBranch<TEvent> secondBranch, 
			@SuppressWarnings("unchecked") PipelineBranch<TEvent>...additionalBranches) {
		Objects.requireNonNull(firstBranch);
		Objects.requireNonNull(secondBranch);
		
		List<PipelineBranch<TEvent>> branches = new ArrayList<>(Arrays.asList(additionalBranches));
		branches.add(0, secondBranch);
		branches.add(0, firstBranch);
		
		return thenBranch(pipelineSection, branches);
	}
	
	static <TEvent> PipelineTopology<TEvent> thenBranch(
			PipelineTopology<TEvent> pipelineSection,
			Collection<PipelineBranch<TEvent>> branches) {		
		PipelineTopology<TEvent> pipeline = pipelineSection;
		int attachIndex = pipelineSection.size();
	
		for (PipelineBranch<TEvent> branch : branches) {
			pipeline = pipeline.combineWith(branch.getBranch(), attachIndex);
		}
		return pipeline;
	}
	
	static <TEvent> PipelineProcess<TEvent> createProducerProcess(Runnable process, Clock clock) {
		return new ProducingPipelineProcessImpl<>(process, clock);
	}
	
	static <TEvent> ConsumingPipelineProcess<TEvent> createConsumerProcess(EventProcessor process, Clock clock) {
		return new ConsumingPipelineProcessImpl<>(process, clock);
	}
	
}
