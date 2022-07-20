package Filtering.pipeline_builder;

import com.hazelcast.jet.pipeline.Pipeline;

import java.util.ArrayList;

public interface PipelineBuilder {

    Pipeline buildPipeline(ArrayList<String> directories);
}
