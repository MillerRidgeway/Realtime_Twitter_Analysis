package Bolt;


import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

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
    private int windowSize;
    private int bCurrent = 1;
    private int entryCount = 0;

    public HashtagCounterBolt(int windowSize){
        this.windowSize = windowSize;
    }

    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        this.counterMap = new HashMap<String, LossyHashtag>();
        this.collector = collector;
    }

    @Override
    public void execute(Tuple tuple) {
        String key = tuple.getString(0);

        //Insertion
        entryCount++;
        if(!counterMap.containsKey(key)){
            LossyHashtag newEntry = new LossyHashtag(key, 1, bCurrent - 1);
            counterMap.put(key, newEntry);
        }else{
            LossyHashtag existingEntry = counterMap.get(key);
            existingEntry.setFrequency(existingEntry.getFrequency() + 1);
            counterMap.put(key, existingEntry);
        }

        //Prune
        if(entryCount % windowSize == 0){
            Iterator mapIter = counterMap.entrySet().iterator();
            while(mapIter.hasNext()){
                LossyHashtag temp = (LossyHashtag) ((Map.Entry<String, LossyHashtag>) mapIter.next()).getValue();
                if(temp.getFrequency() + temp.getDelta() <= bCurrent)
                    mapIter.remove();
            }
            bCurrent++;
        }


        collector.ack(tuple);
        collector.emit(new Values(counterMap.get(key)));
    }

    @Override
    public void cleanup() {
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("hashtag"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

}