package Counting;

import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.aggregate.AggregateOperations;
import com.hazelcast.jet.pipeline.BatchSource;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.Sources;
import com.hazelcast.jet.pipeline.test.TestSources;


import java.io.Serializable;
import java.nio.file.Files;

import java.time.format.DateTimeFormatter;
import java.util.Locale;

public class BatchSourceCounting {

    private static Pipeline buildPipeline(String sourceDir) {
        Pipeline p = Pipeline.create();

        BatchSource<Summary_Mentions> source = Sources.filesBuilder(sourceDir)
                .glob("*.csv")
                .build(path -> Files.lines(path).skip(1).map(Summary_Mentions::parse));

        p.readFrom(source)
                .filter(record -> record.getMentions().equalsIgnoreCase("@PalmerReport"))
                .peek()
                .aggregate(AggregateOperations.counting())
                .writeTo(Sinks.logger());
        return p;
    }

    private static Pipeline mentionStatistics(String sourceDir){
        Pipeline p = Pipeline.create();
        BatchSource<Summary_Mentions> source = Sources.filesBuilder(sourceDir)
                .glob("*.csv")
                .build(path -> Files.lines(path).skip(1).map(Summary_Mentions::parse));




        return p;
    }

    public static void main(String[] args) {

        final String sourceDir = "F:\\archive\\Summary_Mentions\\2022_01";

        Pipeline p = buildPipeline(sourceDir);

        JetInstance instance = Jet.bootstrappedInstance();
        try {
            instance.newJob(p).join();
        } finally {
            Jet.shutdownAll();
        }
    }

    /**
     * Immutable data transfer object mapping the Summary mention.
     */
    private static class Summary_Mentions implements Serializable {
        private String Tweet_ID;
        private String Mentions;

        public static Summary_Mentions parse(String line) {
            String[] split = line.split(",");
            Summary_Mentions record = new Summary_Mentions();
            record.Tweet_ID = split[0];
            record.Mentions = split[1];
            return record;
        }

        @Override
        public String toString() {
            return "Summary_Mentions{" +
                    "Tweet_ID='" + Tweet_ID + '\'' +
                    ", Mentions='" + Mentions + '\'' +
                    '}';
        }

        public String getTweet_ID() {
            return Tweet_ID;
        }

        public void setTweet_ID(String tweet_ID) {
            Tweet_ID = tweet_ID;
        }

        public String getMentions() {
            return Mentions;
        }

        public void setMentions(String mentions) {
            Mentions = mentions;
        }
    }
}