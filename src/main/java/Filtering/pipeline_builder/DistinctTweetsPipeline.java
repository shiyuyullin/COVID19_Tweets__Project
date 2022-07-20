package Filtering.pipeline_builder;

import Filtering.entity.Hashtag;
import com.hazelcast.jet.pipeline.*;

import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

public class DistinctTweetsPipeline implements PipelineBuilder{
    @Override
    public Pipeline buildPipeline(ArrayList<String> directories) {
        Pipeline pipeline = Pipeline.create();
        ArrayList<BatchSource<Hashtag>> batchSourceArrayList = new ArrayList<>();
        for(String dir : Objects.nonNull(directories) ? directories : getHashtagDirectories()){
            batchSourceArrayList.add(Sources.filesBuilder(dir)
                    .glob("2022_01_01*.csv")
                    .build(path -> Files.lines(path).skip(1).map(Hashtag::parse)));
        }
        BatchStage<Hashtag> batchStage = pipeline.readFrom(batchSourceArrayList.get(0));
        for(int i=1;i<batchSourceArrayList.size();i++){
            batchStage = batchStage.merge(pipeline.readFrom(batchSourceArrayList.get(i)));
        }
        batchStage
                .groupingKey(hashtag -> hashtag.hastag)
                .distinct()
                .writeTo(Sinks.list("distinctTweets"));
        return batchStage.getPipeline();

    }
    //    Static directories for testing
    public static List<String> getHashtagDirectories(){
        return Arrays.asList("M:\\Concordia\\Summer 2022\\COMP 6231\\project\\archive (5)\\Summary_Hashtag\\2022_01"
                ,"M:\\Concordia\\Summer 2022\\COMP 6231\\project\\archive (5)\\Summary_Hashtag\\2022_02"
                ,"M:\\Concordia\\Summer 2022\\COMP 6231\\project\\archive (5)\\Summary_Hashtag\\2022_03");
    }
}