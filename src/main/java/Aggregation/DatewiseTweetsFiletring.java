package Aggregation;

import Filtering.entity.Hashtag;
import com.hazelcast.jet.pipeline.*;

import java.nio.file.Files;
import java.time.YearMonth;
import java.util.*;

public class DatewiseTweetsFiletring {

    public Pipeline buildPipeline(ArrayList<String> directories){
        Pipeline pipeline = Pipeline.create();
        ArrayList<BatchSource<Hashtag>> batchSourceArrayList = new ArrayList<>();
        for(String dir : Objects.nonNull(directories) ? directories : getHashtagDirectories()){
            batchSourceArrayList.add(Sources.filesBuilder(dir)
                    .glob("2022_01_01_00_Summary_Hashtag*.csv")
                    .build(path -> Files.lines(path).skip(1).map(line -> {
                        String[] split = line.split(",");
                        String[] directoryPath = dir.split("\\\\");
                        String[] yearMonthArray = directoryPath[directoryPath.length-1].split("_");
                        YearMonth yearMonth = YearMonth.of(Integer.parseInt(yearMonthArray[0]),Integer.parseInt(yearMonthArray[1]));
                        return new Hashtag(split[0], split[1], yearMonth.getYear(), yearMonth.getMonth().getValue());
                    })));
        }
        BatchStage<Hashtag> batchStage = pipeline.readFrom(batchSourceArrayList.get(0));
        for(int i=1;i<batchSourceArrayList.size();i++){
            batchStage = batchStage.merge(pipeline.readFrom(batchSourceArrayList.get(i)));
        }

        batchStage.
                writeTo(Sinks.mapWithMerging("tweetYearMonthTweetNameMap",
                        Hashtag::getYearMonth,
                        Hashtag::getHastag,
                        (oldValue, newValue) -> oldValue + ", " + newValue));
        return pipeline;
    }

    public static List<String> getHashtagDirectories(){
        return Arrays.asList("M:\\Concordia\\Summer 2022\\COMP 6231\\project\\Dataset\\Summary_Hashtag\\2022_01");
//                ,"M:\\Concordia\\Summer 2022\\COMP 6231\\project\\archive (5)\\Summary_Hashtag\\2022_02"
//                ,"M:\\Concordia\\Summer 2022\\COMP 6231\\project\\archive (5)\\Summary_Hashtag\\2022_03");
    }

}
