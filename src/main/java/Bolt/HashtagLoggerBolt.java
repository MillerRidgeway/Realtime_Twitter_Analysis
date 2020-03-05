package Bolt;

import org.apache.storm.Config;
import org.apache.storm.Constants;
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
    private BufferedWriter writer;
    private String logLocation;

    public HashtagLoggerBolt() throws IOException {
        this.logLocation = "/s/chopin/b/grad/millerr/storm_logs/log.txt";
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
        if(isTickTuple(tuple) && hashtagCounts.size() != 0){

        }
    }

    @Override
    public void cleanup() {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }

    protected static boolean isTickTuple(Tuple tuple) {
        return tuple.getSourceComponent().equals(Constants.SYSTEM_COMPONENT_ID)
                && tuple.getSourceStreamId().equals(Constants.SYSTEM_TICK_STREAM_ID);
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        // configure how often a tick tuple will be sent to our bolt
        Config conf = new Config();
        conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, 10);
        return conf;
    }
}
