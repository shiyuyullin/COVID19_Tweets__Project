package Counting;

import Models.Summary_Mentions;
import com.hazelcast.function.ComparatorEx;
import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.map.IMap;
import pipeline_builder.Covid19HashtagPipeline;
import com.hazelcast.jet.aggregate.AggregateOperations;
import com.hazelcast.jet.pipeline.BatchSource;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.Sources;
import com.hazelcast.jet.pipeline.test.TestSources;


import java.io.Serializable;
import java.nio.file.Files;


import java.io.Serializable;
import java.nio.file.Files;
import java.time.format.DateTimeFormatter;
import java.util.Comparator;
import java.util.Locale;
import java.util.Map;

public class BatchSourceCounting {

    private static Pipeline mentionCount(String sourceDir, String mention) {
        Pipeline p = Pipeline.create();

        BatchSource<Summary_Mentions> source = Sources.filesBuilder(sourceDir)
                .glob("*.csv")
                .build(path -> Files.lines(path).skip(1).map(Summary_Mentions::parse));

        p.readFrom(source)
                .filter(record -> record.getMentions().equalsIgnoreCase(mention))
                .aggregate(AggregateOperations.counting())
                .writeTo(Sinks.logger());
        return p;
    }

    private static Pipeline mentionStatistics(String sourceDir){
        Pipeline p = Pipeline.create();
        BatchSource<Summary_Mentions> source = Sources.filesBuilder(sourceDir)
                .glob("*.csv")
                .build(path -> Files.lines(path).skip(1).map(Summary_Mentions::parse));
        // Count the top 10 mentions
        p.readFrom(source)
                .groupingKey(Summary_Mentions::getMentions)
                .aggregate(AggregateOperations.counting())
                .sort((ComparatorEx<Map.Entry<String, Long>>) (stringLongEntry, t1) -> stringLongEntry.getValue().compareTo(t1.getValue()))
                .peek()
                .writeTo(Sinks.map("statistics"));
        return p;
    }

    public static void main(String[] args) {

        final String sourceDir = "F:\\archive\\Summary_Mentions\\2022_01_subset";

        Pipeline p = mentionCount(sourceDir, "@maaiavilaa");
//        Pipeline p = mentionStatistics(sourceDir);
        JetInstance instance = Jet.bootstrappedInstance();
        Jet.newJetInstance();
        try {
            instance.newJob(p).join();
            IMap<String, Long> count = instance.getMap("statistics");
        } finally {
            Jet.shutdownAll();
        }
    }

}