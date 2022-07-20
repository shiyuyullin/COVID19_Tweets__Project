package Models;

import java.io.Serializable;

public class Tweet implements Serializable {

    private String Tweet_ID;
    private String Language;
    private String Geolocation_coordinate;
    private String RT;
    private long Likes;
    private long Retweets;
    private String Country;
    private String Date_Created;

    public Tweet() {
        Tweet_ID = "";
        Language = "";
        Geolocation_coordinate = "";
        RT = "";
        Likes = 0;
        Retweets = 0;
        Country = "";
        Date_Created = "";
    }

    public static Tweet parse(String line){
        String[] split = line.split(",");
        Tweet record = new Tweet();
        record.Tweet_ID = split[0];
        record.Language = split[1];
        record.Geolocation_coordinate = split[2];
        record.RT = split[3];
        record.Likes = Long.parseLong(split[4]);
        record.Retweets = Long.parseLong(split[5]);
        record.Country = split[6];
        record.Date_Created = split[7];
        return record;
    }



    @Override
    public String toString() {
        return "Tweet{" +
                "Tweet_ID='" + Tweet_ID + '\'' +
                ", Language='" + Language + '\'' +
                ", Likes=" + Likes +
                ", Retweets=" + Retweets +
                '}';
    }

    public String getTweet_ID() {
        return Tweet_ID;
    }

    public void setTweet_ID(String tweet_ID) {
        Tweet_ID = tweet_ID;
    }

    public String getLanguage() {
        return Language;
    }

    public void setLanguage(String language) {
        Language = language;
    }

    public String getGeolocation_coordinate() {
        return Geolocation_coordinate;
    }

    public void setGeolocation_coordinate(String geolocation_coordinate) {
        Geolocation_coordinate = geolocation_coordinate;
    }

    public String getRT() {
        return RT;
    }

    public void setRT(String RT) {
        this.RT = RT;
    }

    public long getLikes() {
        return Likes;
    }

    public void setLikes(long likes) {
        Likes = likes;
    }

    public long getRetweets() {
        return Retweets;
    }

    public void setRetweets(long retweets) {
        Retweets = retweets;
    }

    public String getCountry() {
        return Country;
    }

    public void setCountry(String country) {
        Country = country;
    }

    public String getDate_Created() {
        return Date_Created;
    }

    public void setDate_Created(String date_Created) {
        Date_Created = date_Created;
    }
}
