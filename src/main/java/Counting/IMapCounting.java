package Counting;

import Models.Summary_Mentions;
import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Util;
import com.hazelcast.jet.aggregate.AggregateOperations;
import com.hazelcast.jet.pipeline.*;
import com.hazelcast.map.IMap;

import java.nio.file.Files;
import java.util.Map;

public class IMapCounting {

    public static JetInstance jet;
    public static int globalCounter = 0;
    public static final String SOURCE_DIR_2022_01 = "F:\\archive\\Summary_Mentions\\2022_01_subset";

    public static void populateMap(String mapName, String directory){

        Pipeline p = Pipeline.create();
        // Building a batch source from csv files
        BatchSource<Map.Entry<Integer, Summary_Mentions>> source = Sources.filesBuilder(directory)
                .glob("*.csv")
                .sharedFileSystem(true)
                .build(path -> Files.lines(path).skip(1).map(line -> {
                            String[] split = line.split(",");
                            Summary_Mentions record = new Summary_Mentions();
                            record.setTweet_ID(split[0]);
                            record.setMentions(split[1]);
                            return Util.entry(globalCounter++, record);
                        }
                ));
        // Dump file content into an IMap
        p.readFrom(source).writeTo(Sinks.map("Summary_Mentions"));
        jet.newJob(p).join();
    }

    public static void countMention(String mapName, String mention){
        Pipeline p = Pipeline.create();
        p.readFrom(Sources.map(mapName))
                .filter(mapEntry ->{
                    Summary_Mentions value = (Summary_Mentions) mapEntry.getValue();
                    return value.getMentions().equalsIgnoreCase(mention);
                })
                .peek()
                .rebalance()
                .aggregate(AggregateOperations.counting())
                .writeTo(Sinks.logger());
        jet.newJob(p).join();
    }


    public static void main(String[] args) {

        // Creating two nodes in a cluster
        jet = Jet.newJetInstance();
        Jet.newJetInstance();
        Jet.newJetInstance();
        IMap<Integer, Summary_Mentions> summary_mentions = jet.getMap("Summary_Mentions");
        System.out.println("Populating map...");
        populateMap("Summary_Mentions", SOURCE_DIR_2022_01);
        System.out.println("Map populated");
        String mention = "@PalmerReport";
        System.out.println("Counting how many times " + mention + " is mentioned in 2022-01");
        countMention("Summary_Mentions", mention);
        Jet.shutdownAll();




    }
}
