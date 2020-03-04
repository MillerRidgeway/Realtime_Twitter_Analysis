package Bolt;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class HashtagLoggerBolt implements IRichBolt {
    private OutputCollector collector;
    private Map<String, Integer> hashtagCounts;
    private int windowSize;
    private BufferedWriter writer;
    private String logLocation;

    public HashtagLoggerBolt(int windowSize) throws IOException {
        this.windowSize = windowSize;
        this.logLocation = "/s/chopin/b/grad/millerr/storm_logs";
    }
    @Override
    public void prepare(Map<String, Object> map, TopologyContext topologyContext, OutputCollector outputCollector) {
        try {
            this.writer = new BufferedWriter(new FileWriter(logLocation, true));
        } catch (IOException e) {
            e.printStackTrace();
        }
        this.hashtagCounts = new HashMap<String, Integer>();
        this.collector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {

    }

    @Override
    public void cleanup() {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
