package Models;

import java.io.Serializable;

public class Summary_Sentiment implements Serializable {

    private String Tweet_ID;
    private String Sentiment_Label;
    private double Logits_Neutral;
    private double Logits_Positive;
    private double Logits_Negative;

    public Summary_Sentiment(){
        Tweet_ID = "";
        Sentiment_Label = "";
        Logits_Negative = 0.0;
        Logits_Neutral = 0.0;
        Logits_Positive = 0.0;
    }


    public static Summary_Sentiment parse(String line){
        String[] split = line.split(",");
        Summary_Sentiment record = new Summary_Sentiment();
        record.Tweet_ID = split[0];
        record.Sentiment_Label = split[1];
        record.Logits_Neutral = Double.parseDouble(split[2]);
        record.Logits_Positive = Double.parseDouble(split[3]);
        record.Logits_Negative = Double.parseDouble(split[4]);
        return record;
    }

    @Override
    public String toString() {
        return "Summary_Sentiment{" +
                "Tweet_ID='" + Tweet_ID + '\'' +
                ", Sentiment_Label='" + Sentiment_Label + '\'' +
                '}';
    }

    public String getTweet_ID() {
        return Tweet_ID;
    }

    public void setTweet_ID(String tweet_ID) {
        Tweet_ID = tweet_ID;
    }

    public String getSentiment_Label() {
        return Sentiment_Label;
    }

    public void setSentiment_Label(String sentiment_Label) {
        Sentiment_Label = sentiment_Label;
    }

    public double getLogits_Neutral() {
        return Logits_Neutral;
    }

    public void setLogits_Neutral(double logits_Neutral) {
        Logits_Neutral = logits_Neutral;
    }

    public double getLogits_Positive() {
        return Logits_Positive;
    }

    public void setLogits_Positive(double logits_Positive) {
        Logits_Positive = logits_Positive;
    }

    public double getLogits_Negative() {
        return Logits_Negative;
    }

    public void setLogits_Negative(double logits_Negative) {
        Logits_Negative = logits_Negative;
    }
}
