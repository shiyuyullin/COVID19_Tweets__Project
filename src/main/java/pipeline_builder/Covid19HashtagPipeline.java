package pipeline_builder;

import com.hazelcast.jet.pipeline.*;
import entity.Hashtag;

import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Covid19HashtagPipeline implements PipelineBuilder {


    @Override
    public Pipeline buildPipeline() {
        Pipeline pipeline = Pipeline.create();
        ArrayList<BatchSource<Hashtag>> batchSourceArrayList = new ArrayList<>();
        for(String dir : getHashtagDirectories()){
            batchSourceArrayList.add(Sources.filesBuilder(dir)
                    .glob("*.csv")
                    .build(path -> Files.lines(path).skip(1).map(Hashtag::parse)));
        }
        BatchStage<Hashtag> batchStage = pipeline.readFrom(batchSourceArrayList.get(0));
        for(int i=1;i<batchSourceArrayList.size();i++){
            batchStage = batchStage.merge(pipeline.readFrom(batchSourceArrayList.get(i)));
        }
        batchStage
                .filter(row -> row.getHastag().equalsIgnoreCase("#COVID19"))
                .setName("Filter covid tweets")
                .writeTo(Sinks.files("M:\\Concordia\\Summer 2022\\COMP 6231\\project\\results"));
        return batchStage.getPipeline();
    }

    public static List<String> getHashtagDirectories(){
        return Arrays.asList("M:\\Concordia\\Summer 2022\\COMP 6231\\project\\archive (5)\\Summary_Hashtag\\2022_01"
        ,"M:\\Concordia\\Summer 2022\\COMP 6231\\project\\archive (5)\\Summary_Hashtag\\2022_02"
        ,"M:\\Concordia\\Summer 2022\\COMP 6231\\project\\archive (5)\\Summary_Hashtag\\2022_03");
    }
}
