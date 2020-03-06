package Bolt;


import java.util.*;

import org.apache.storm.Config;
import org.apache.storm.Constants;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;

import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;

public class HashtagCounterBolt implements IRichBolt {
    private Map<String, LossyHashtag> counterMap;
    private OutputCollector collector;
    private double eta;
    private double threshold;
    private double windowSize;
    private int bCurrent = 1;
    private int entryCount = 0;


    public HashtagCounterBolt(double eta, double threshold) {
        this.eta = eta;
        this.threshold = threshold;
        this.windowSize = 1 / eta;
    }

    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        this.counterMap = new HashMap<String, LossyHashtag>();
        this.collector = collector;
    }

    @Override
    public void execute(Tuple tuple) {
        //Forward batch to logger bolt every 10s
        if (isTickTuple(tuple) && counterMap.size() != 0) {
            Map<String, LossyHashtag> sortedCounterMap = counterMap; //Will sort this eventually

            Iterator mapIter = sortedCounterMap.entrySet().iterator();
            while (mapIter.hasNext()) {
                LossyHashtag temp = ((Map.Entry<String, LossyHashtag>) mapIter.next()).getValue();
                //System.out.println("Emitting a hashtag: " + temp);
                collector.emit(new Values(temp));
            }
        } else {
            //Insertion
            String key = tuple.getString(0);
            entryCount++;
            if (!counterMap.containsKey(key)) {
                LossyHashtag newEntry = new LossyHashtag(key, 1, bCurrent - 1);
                counterMap.put(key, newEntry);
            } else {
                LossyHashtag existingEntry = counterMap.get(key);
                existingEntry.setFrequency(existingEntry.getFrequency() + 1);
                counterMap.put(key, existingEntry);
            }

            //Prune
            if (entryCount % windowSize == 0) {
                Iterator mapIter = counterMap.entrySet().iterator();
                while (mapIter.hasNext()) {
                    LossyHashtag temp = ((Map.Entry<String, LossyHashtag>) mapIter.next()).getValue();
                    if (temp.getFrequency() + temp.getDelta() <= bCurrent)
                        mapIter.remove();
                }
                bCurrent++;
            }
        }
    }

    @Override
    public void cleanup() {
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("hashtag"));
    }

    private static boolean isTickTuple(Tuple tuple) {
        return tuple.getSourceComponent().equals(Constants.SYSTEM_COMPONENT_ID)
                && tuple.getSourceStreamId().equals(Constants.SYSTEM_TICK_STREAM_ID);
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        // configure how often a tick tuple will be sent to our bolt
        Config conf = new Config();
        conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, 5);
        return conf;
    }

}