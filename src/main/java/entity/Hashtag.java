package entity;

import java.io.Serializable;

public class Hashtag implements Serializable {

    public String TWEET_ID;

    public String hastag;

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
