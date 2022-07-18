package pipeline_builder;

import com.hazelcast.jet.pipeline.Pipeline;

public interface PipelineBuilder {

    Pipeline buildPipeline();

}
