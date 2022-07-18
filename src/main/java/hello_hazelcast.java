import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import pipeline_builder.Covid19HashtagPipeline;
import com.hazelcast.jet.aggregate.AggregateOperations;
import com.hazelcast.jet.pipeline.BatchSource;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.Sources;

import java.io.Serializable;
import java.nio.file.Files;
import java.time.format.DateTimeFormatter;
import java.util.Locale;

/**
 * Demonstrates the usage of the file {@link Sources#filesBuilder sources}
 * in a job that reads a sales records in a CSV, filters possible contactless
 * transactions, aggregates transaction counts per payment type and prints
 * the results to standard output.
 * <p>
 * The sample CSV file is in {@code {module.dir}/data/sales.csv}.
 */
public class hello_hazelcast {

    private static Pipeline buildPipeline(String sourceDir) {
        Pipeline p = Pipeline.create();

        BatchSource<Summary_Mentions> source = Sources.filesBuilder(sourceDir)
                .glob("2022_01_01_00_Summary_Mentions.csv")
                .build(path -> Files.lines(path).skip(1).map(Summary_Mentions::parse));



        p.readFrom(source)
                .filter(record -> record.getMentions().equalsIgnoreCase("@PalmerReport"))
                .aggregate(AggregateOperations.counting())
                .writeTo(Sinks.logger());

        return p;
    }

    public static void main(String[] args) {

        final String sourceDir = "C:\\Users\\Shiyu\\Desktop\\archive\\Summary_Mentions\\2022_01";

        Pipeline p = buildPipeline(sourceDir);

        JetInstance instance = Jet.bootstrappedInstance();
        try {
            instance.newJob(p).join();
            Covid19HashtagPipeline covid19HashtagPipeline = new Covid19HashtagPipeline();
            instance.newJob(covid19HashtagPipeline.buildPipeline()).join();
        } finally {
            Jet.shutdownAll();
        }
    }

    /**
     * Immutable data transfer object mapping the Summary mention.
     */
    private static class Summary_Mentions implements Serializable {
        private static final DateTimeFormatter DATE_TIME_FORMATTER =
                DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss", Locale.US);

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
            return "SalesRecordLine{" +
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