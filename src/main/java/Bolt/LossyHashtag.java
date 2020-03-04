package Bolt;

public class LossyHashtag {
    private String hashtag;
    private int frequency;
    private int delta;

    public LossyHashtag(String hashtag, int frequency, int delta){
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
}
