package Filtering.entity;

import java.io.Serializable;
import java.time.YearMonth;

public class Hashtag implements Serializable {

    public Hashtag(){
    };

    public Hashtag(String TWEET_ID, String hastag, Integer year, Integer month){
        this.hastag = hastag;
        this.TWEET_ID = TWEET_ID;
        this.month = month;
        this.year = year;
    }

    public String TWEET_ID;

    public String hastag;

    public Integer year;

    public Integer month;

    public void setTWEET_ID(String TWEET_ID){
        this.TWEET_ID = TWEET_ID;
    }

    public void setHastag(String hastag){
        this.hastag = hastag;
    }

    public String getTWEET_ID(){
        return this.TWEET_ID;
    }

    public String getHastag(){
        return this.hastag;
    }

    public Integer getYear(){
        return this.year;
    }

    public Integer getMonth(){
        return this.month;
    }

    public void setYear(Integer year){
        this.year = year;
    }

    public void setMonth(Integer month){
        this.month = month;
    }

    public String getYearMonth(){
        return YearMonth.of(getYear(), getMonth()).toString();
    }

    public static Hashtag parse(String line) {
        String[] split = line.split(",");
        Hashtag record = new Hashtag();
        record.TWEET_ID = split[0];
        record.hastag = split[1];
        return record;
    }

    @Override
    public String toString(){
        return "Tweet Id -> " + getTWEET_ID() + ", Hashtag -> " + getHastag();
    }


}
