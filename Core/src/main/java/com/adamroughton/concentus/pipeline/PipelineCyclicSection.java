package com.adamroughton.concentus.pipeline;


import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.Executor;

import com.adamroughton.concentus.Clock;
import com.lmax.disruptor.EventProcessor;
import com.lmax.disruptor.RingBuffer;

public class PipelineCyclicSection<TEvent> {

	public static class CyclicStarter<TEvent>  {

		private final RingBuffer<TEvent> _cycleConnector;
		private final PipelineTopology<TEvent> _pipelineSegment;
		private final Clock _clock;
		
		CyclicStarter(RingBuffer<TEvent> connector, Clock clock) {
			_cycleConnector = connector;
			_pipelineSegment = new PipelineTopology<>();
			_clock = clock;
		}
		
		public CyclicBuilder<TEvent> then(ConsumingPipelineProcess<TEvent> process) {
			_cycleConnector.setGatingSequences(process.getSequence());
			PipelineSegment<TEvent> segment = new PipelineSegment<>(Arrays.asList(_cycleConnector), process, _clock);
			PipelineTopology<TEvent> topology = _pipelineSegment.add(0, segment);
			return new CyclicBuilder<>(segment, new ProcessingPipeline.Builder<>(1, topology, _clock), _clock);
		}
		
		public CyclicBuilder<TEvent> then(EventProcessor process) {
			return then(ProcessingPipeline.<TEvent>createConsumerProcess(process, _clock));
		}
		
		@SafeVarargs
		public final CyclicPipelineSectionJoin<TEvent> join(PipelineSection<TEvent> first, PipelineSection<TEvent>...additional) {
			PipelineSection<TEvent> cycleAsSection = new PipelineSection<>(0, _cycleConnector, _pipelineSegment, _clock);
			Collection<PipelineSection<TEvent>> joinSections = ProcessingPipeline.getJoinSections(cycleAsSection, first, additional);
			ProcessingPipeline.PipelineSectionJoin<TEvent> sectionJoin = new ProcessingPipeline.PipelineSectionJoin<>(joinSections, _clock);
			return new CyclicPipelineSectionJoin<>(sectionJoin, _clock);
		}
		
	}
	
	public static class CyclicBuilder<TEvent> {
		
		private final PipelineSegment<TEvent> _cycleLink;
		private final ProcessingPipeline.Builder<TEvent> _builder;
		private final Clock _clock;
		
		CyclicBuilder(PipelineSegment<TEvent> cycleLink, ProcessingPipeline.Builder<TEvent> builder, Clock clock) {
			_cycleLink = cycleLink;
			_builder = builder;
			_clock = clock;
		}
		
		public CyclicConnector<TEvent> thenConnector(RingBuffer<TEvent> connector) {
			return new CyclicConnector<>(_cycleLink, _builder.thenConnector(connector), _clock);
		}
		
		public final CyclicBuilder<TEvent> attachBranch(PipelineBranch<TEvent> branch) {
			return new CyclicBuilder<>(_cycleLink, _builder.attachBranch(branch), _clock);
		}
		
		@SafeVarargs
		public final CyclicBuilder<TEvent> attachBranches(PipelineBranch<TEvent> firstBranch, PipelineBranch<TEvent>...additional) {
			return new CyclicBuilder<>(_cycleLink, _builder.attachBranches(firstBranch, additional), _clock);
		}
		
		public ProcessingPipeline<TEvent> completeCycle(Executor executor) {
			PipelineTopology<TEvent> cyclePipeline = _builder._pipelineSection.add(_builder._layerIndex, _cycleLink);
			return new ProcessingPipeline<TEvent>(executor, cyclePipeline, _clock);
		}
		
	}
	
	public static class CyclicConnector<TEvent> {
		
		private final PipelineSegment<TEvent> _cycleLink;
		private final ProcessingPipeline.Connector<TEvent> _connector;
		private final Clock _clock;
		
		CyclicConnector(PipelineSegment<TEvent> cycleLink, ProcessingPipeline.Connector<TEvent> connector, Clock clock) {
			_cycleLink = cycleLink;
			_connector = connector;
			_clock = clock;
		}
		
		public CyclicBuilder<TEvent> then(ConsumingPipelineProcess<TEvent> process) {
			return new CyclicBuilder<>(_cycleLink, _connector.then(process), _clock);
		}
		
		public CyclicBuilder<TEvent> then(EventProcessor process) {
			return new CyclicBuilder<>(_cycleLink, _connector.then(process), _clock);
		}
		
		@SafeVarargs
		public final CyclicPipelineSectionJoin<TEvent> join(PipelineSection<TEvent> first, PipelineSection<TEvent>...additional) {
			return new CyclicPipelineSectionJoin<>(_cycleLink, _connector.join(first, additional), _clock);
		}
		
	}
	
	public static class CyclicPipelineSectionJoin<TEvent> {

		private final PipelineSegment<TEvent> _cycleLink;
		private final ProcessingPipeline.PipelineSectionJoin<TEvent> _pipelineSectionJoin;
		private final Clock _clock;
		
		private final boolean _linkCreated;
		
		CyclicPipelineSectionJoin(
				PipelineSegment<TEvent> cycleLink,
				ProcessingPipeline.PipelineSectionJoin<TEvent> pipelineSectionJoin,
				Clock clock) {
			_cycleLink = cycleLink;
			_pipelineSectionJoin = pipelineSectionJoin;
			_clock = clock;
			_linkCreated = false;
		}
		
		CyclicPipelineSectionJoin(
				ProcessingPipeline.PipelineSectionJoin<TEvent> pipelineSectionJoin,
				Clock clock) {
			_cycleLink = null;
			_pipelineSectionJoin = pipelineSectionJoin;
			_clock = clock;
			_linkCreated = true;
		}
		
		public CyclicBuilder<TEvent> into(ConsumingPipelineProcess<TEvent> process) {
			if (_linkCreated) {
				PipelineSegment<TEvent> cycleSegment = _pipelineSectionJoin.createSegment(process);
				PipelineTopology<TEvent> pipelineSection = _pipelineSectionJoin._pipeline.add(_pipelineSectionJoin._layerIndex, cycleSegment);
				return new CyclicBuilder<>(_cycleLink, new ProcessingPipeline.Builder<>(_pipelineSectionJoin._layerIndex + 1, pipelineSection, _clock), _clock);
			} else {
				return new CyclicBuilder<>(_cycleLink, _pipelineSectionJoin.into(process), _clock);
			}
		}
		
		public CyclicBuilder<TEvent> into(EventProcessor process) {
			return into(ProcessingPipeline.<TEvent>createConsumerProcess(process, _clock));
		}
		
	}
	
}
