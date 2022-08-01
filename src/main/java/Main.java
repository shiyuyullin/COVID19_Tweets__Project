import Aggregating.MostRetweetedAndSentiment;
import Aggregation.DatewiseTweetsFiletring;
import Counting.BatchSourceCounting;
import Counting.IMapCounting;
import Filtering.pipeline_builder.DistinctTweetsPipeline;
import Filtering.pipeline_builder.TweetFilterPipeline;
import com.hazelcast.collection.IList;
import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.map.IMap;
import org.apache.commons.lang3.time.StopWatch;

import java.time.YearMonth;
import java.util.*;

// This would be the Main entry point of the program
public class Main {

    static JetInstance jetInstance = Jet.newJetInstance();
    static JetInstance jetInstance1 = Jet.newJetInstance();
    final static String  getSourceDirSummaryHashtags = "M:\\Concordia\\Summer 2022\\COMP 6231\\project\\Dataset\\Summary_Hashtag\\2022_01";
    final static String sourceDirSummaryDetails = "M:\\Concordia\\Summer 2022\\COMP 6231\\project\\Dataset\\Summary_Details\\2022_01";
    final static String sourceDirSummarySentiments = "M:\\Concordia\\Summer 2022\\COMP 6231\\project\\Dataset\\Summary_Sentiment\\2022_01";
    static final String sourceDirSummaryMentions = "M:\\Concordia\\Summer 2022\\COMP 6231\\project\\Dataset\\Summary_Mentions\\2022_01";
    public static void main(String[] args) {
       StopWatch stopWatch = new StopWatch();
       stopWatch.start();
       processData();
       printStats();
       Jet.shutdownAll();
       stopWatch.stop();
       System.out.println("Total time is -> " + stopWatch.getTime());
    }

    public static void processData(){
        try {
            TweetFilterPipeline tweetFilterPipeline = new TweetFilterPipeline();
            jetInstance.newJob(tweetFilterPipeline.buildPipeline(new ArrayList<>(){{add(getSourceDirSummaryHashtags);}})).join();
            DistinctTweetsPipeline distinctTweetsPipeline = new DistinctTweetsPipeline();
            jetInstance.newJob(distinctTweetsPipeline.buildPipeline(new ArrayList<>(){{add(getSourceDirSummaryHashtags);}})).join();
            DatewiseTweetsFiletring datewiseTweetsFiletring = new DatewiseTweetsFiletring();
            jetInstance.newJob(datewiseTweetsFiletring.buildPipeline(new ArrayList<>(){{add(getSourceDirSummaryHashtags);}})).join();
            jetInstance.newJob(MostRetweetedAndSentiment.topNRetweeted(20,sourceDirSummaryDetails)).join();
            System.out.println("Populating map...");
            jetInstance.newJob(IMapCounting.buildPipelineForPopulation("Summary_Mentions", sourceDirSummaryMentions)).join();

            jetInstance.newJob(BatchSourceCounting.mentionStatistics(sourceDirSummaryMentions)).join();
        }
        catch (Exception e){
            e.printStackTrace();
        }
    }

    public static void printStats(){
        String[] covid19TweetsIds = jetInstance.getMap("tweetNameTweetIdsMap").get("#Covid19").toString().split(",");
        System.out.println("Stats after Filtering data are as below ->");
        System.out.println("Total " + covid19TweetsIds.length + " people tweeted with Hashtag Covid19" + "\n" +
                "Some Sample Ids are -> ");
        int limit = 10;
        int counter = 1;
        for(String s : covid19TweetsIds){
            if(counter > limit) break;
            System.out.println(counter + ". " + s);
            counter++;
        }

        IList<Object> distinceTweets = jetInstance.getList("distinctTweets");
        System.out.println("Total number of unique tweets are -> " + distinceTweets.size());

        System.out.println();

        IMap<YearMonth, String> dateWiseTweetsFiletring = jetInstance.getMap("tweetYearMonthTweetNameMap");
        for(Map.Entry<YearMonth, String> entry : dateWiseTweetsFiletring.entrySet()){
            System.out.println("Tweets for year month " + entry.getKey() + " are");
            String[] tweets = entry.getValue().split(",");
            for(String s : tweets){
                System.out.println(s);
            }
        }

        IList<String> top5MostRetweets = jetInstance.getList("Top5_Retweeted_Tweets");
        System.out.println("Top 20 Most Re-Tweeted Tweets are -> ");
        for(String s : top5MostRetweets){
            System.out.println(s);
        }
        System.out.println();
        jetInstance.newJob(MostRetweetedAndSentiment.topNRetweetedSentiment(sourceDirSummarySentiments, top5MostRetweets)).join();

        System.out.println("Map populated");
        String mention = "@FacesOfCOVID";
        System.out.println("Counting how many times " + mention + " is mentioned in 2022-01");
        jetInstance.newJob(IMapCounting.buildPipelineForCounting("Summary_Mentions", mention)).join();

        IMap<String, Long> mentionsMap = jetInstance.getMap("statistics");

        System.out.println("Top 10 mentions are - > ");
        List<Map.Entry<String, Long>> list = mentionsMap.entrySet().stream().sorted(Comparator.comparing(Map.Entry<String, Long>::getValue).reversed()).limit(10).toList();
        for(Map.Entry<String,Long> entry : list){
            System.out.println(entry.getKey() + " " + entry.getValue());
        }
    }

}
