package models;

import java.io.Serializable;

public class Summary_Mentions implements Serializable {

    private String Tweet_ID;
    private String Mentions;

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