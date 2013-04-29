package com.adamroughton.concentus.pipeline;

import com.lmax.disruptor.EventProcessor;

public interface ConsumingPipelineProcess<TEvent> extends PipelineProcess<TEvent>, Runnable, EventProcessor {

}
