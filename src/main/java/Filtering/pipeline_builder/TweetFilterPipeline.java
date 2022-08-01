package Filtering.pipeline_builder;

import com.hazelcast.jet.pipeline.*;
import Filtering.entity.*;

import java.nio.file.Files;
import java.util.*;

public class TweetFilterPipeline implements PipelineBuilder {


    @Override
    public Pipeline buildPipeline(ArrayList<String> directories) {
        Pipeline pipeline = Pipeline.create();
        ArrayList<BatchSource<Hashtag>> batchSourceArrayList = new ArrayList<>();
        for(String dir : Objects.nonNull(directories) ? directories : getHashtagDirectories()){
            batchSourceArrayList.add(Sources.filesBuilder(dir)
                    .glob("*.csv")
                    .build(path -> Files.lines(path).skip(1).map(Hashtag::parse)));
        }
        BatchStage<Hashtag> batchStage = pipeline.readFrom(batchSourceArrayList.get(0));
        for(int i=1;i<batchSourceArrayList.size();i++){
            batchStage = batchStage.merge(pipeline.readFrom(batchSourceArrayList.get(i)));
        }
        batchStage
                .setName("Filter Tweets to Tweet Ids")
                .writeTo(Sinks.mapWithMerging("tweetNameTweetIdsMap",
                        Hashtag::getHastag,
                        Hashtag::getTWEET_ID,
                        (oldValue, newValue) -> oldValue + ", " + newValue)
                );
        return batchStage.getPipeline();
    }

//    Static directories for testing
    public static List<String> getHashtagDirectories(){
        return Arrays.asList("M:\\Concordia\\Summer 2022\\COMP 6231\\project\\Dataset\\Summary_Hashtag\\2022_01");
//        ,"M:\\Concordia\\Summer 2022\\COMP 6231\\project\\archive (5)\\Summary_Hashtag\\2022_02"
//        ,"M:\\Concordia\\Summer 2022\\COMP 6231\\project\\archive (5)\\Summary_Hashtag\\2022_03");
    }
}
