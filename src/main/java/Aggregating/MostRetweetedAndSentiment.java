package Aggregating;

import Models.Summary_Sentiment;
import Models.Tweet;
import com.hazelcast.collection.IList;
import com.hazelcast.function.ComparatorEx;
import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Traversers;
import com.hazelcast.jet.aggregate.AggregateOperations;
import com.hazelcast.jet.pipeline.*;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;

public class MostRetweetedAndSentiment {

    public static Pipeline topNRetweeted(int n, String sourceDir){
        Pipeline p1 = Pipeline.create();
        Pipeline p2 = Pipeline.create();
        // Build a batch source
        BatchSource<Tweet> source = Sources.filesBuilder(sourceDir)
                .glob("*.csv")
                .sharedFileSystem(true)
                .build(path -> Files.lines(path).skip(1).map(Tweet::parse));
        // Getting the top N retweeted tweets
        p1.readFrom(source)
                .aggregate(AggregateOperations.topN(n, ComparatorEx.comparing(Tweet::getRetweets)))
                .flatMap(Traversers::traverseIterable)
                .peek()
                .map(Tweet::getTweet_ID)
                .writeTo(Sinks.list("Top5_Retweeted_Tweets"));
        return p1;
    }

    public static Pipeline topNRetweetedSentiment(String sourceDir, IList<String> list){
        Pipeline p = Pipeline.create();
        BatchSource<Summary_Sentiment> source = Sources.filesBuilder(sourceDir)
                .glob("*.csv")
                .sharedFileSystem(true)
                .build(path -> Files.lines(path).skip(1).map(Summary_Sentiment::parse));
        ArrayList<String> temp = new ArrayList<>(list);
        p.readFrom(source)
                .filter(record -> temp.contains(record.getTweet_ID()))
                .writeTo(Sinks.logger());
        return p;
    }


    public static void main(String[] args) {

        final String sourceDir = "F:\\archive\\Summary_Details\\2022_01";
        final String sourceDir1 = "F:\\archive\\Summary_Sentiment\\2022_01";
        JetInstance instance = Jet.newJetInstance();
        Jet.newJetInstance();
        Jet.newJetInstance();
        Pipeline p = topNRetweeted(20, sourceDir);
        instance.newJob(p).join();
        Pipeline p2 = topNRetweetedSentiment(sourceDir1,instance.getList("Top5_Retweeted_Tweets"));
        instance.newJob(p2).join();
        Jet.shutdownAll();


    }
}
