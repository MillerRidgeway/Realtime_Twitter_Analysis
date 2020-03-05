package Bolt;

import java.io.Serializable;

public class LossyHashtag implements Serializable, Comparable {
    private String hashtag;
    private int frequency;
    private int delta;

    public LossyHashtag(String hashtag, int frequency, int delta) {
        this.hashtag = hashtag;
        this.frequency = frequency;
        this.delta = delta;
    }

    public String getHashtag() {
        return hashtag;
    }

    public void setHashtag(String hashtag) {
        this.hashtag = hashtag;
    }

    public int getFrequency() {
        return frequency;
    }

    public void setFrequency(int frequency) {
        this.frequency = frequency;
    }

    public int getDelta() {
        return delta;
    }

    public void setDelta(int delta) {
        this.delta = delta;
    }

    @Override
    public String toString() {
        return "Hashtag: " + this.hashtag + ", Freq: " + this.frequency + ", Delta:" + this.delta;
    }

    @Override
    public int compareTo(Object o) {
        LossyHashtag temp = (LossyHashtag) o;
        if (this.frequency < temp.frequency)
            return -1;
        else if (this.frequency > temp.frequency)
            return 1;
        else
            return 0;
    }
}
