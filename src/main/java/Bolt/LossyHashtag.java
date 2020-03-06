package Bolt;

import java.io.Serializable;
import java.util.*;

public class LossyHashtag implements Serializable, Comparable {
    private String hashtag;
    private int frequency;
    private int delta;

    LossyHashtag(String hashtag, int frequency, int delta) {
        this.hashtag = hashtag;
        this.frequency = frequency;
        this.delta = delta;
    }

    String getHashtag() {
        return hashtag;
    }

    public void setHashtag(String hashtag) {
        this.hashtag = hashtag;
    }

    int getFrequency() {
        return frequency;
    }

    void setFrequency(int frequency) {
        this.frequency = frequency;
    }

    int getDelta() {
        return delta;
    }

    public void setDelta(int delta) {
        this.delta = delta;
    }

    String toLogEntry(){
     return "<" + getHashtag() + ">";
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

    static Map<String, LossyHashtag> sortByValues(Map<String, LossyHashtag> map) {
        List list = new LinkedList(map.entrySet());
        // Defined Custom Comparator here
        Collections.sort(list, new Comparator() {
            public int compare(Object o1, Object o2) {
                return ((Comparable) ((Map.Entry) (o1)).getValue())
                        .compareTo(((Map.Entry) (o2)).getValue());
            }
        });

        // Here I am copying the sorted list in HashMap
        // using LinkedHashMap to preserve the insertion order
        HashMap sortedHashMap = new LinkedHashMap();
        for (Iterator it = list.iterator(); it.hasNext(); ) {
            Map.Entry entry = (Map.Entry) it.next();
            sortedHashMap.put(entry.getKey(), entry.getValue());
        }
        return sortedHashMap;
    }
}

