package Bolt;

import org.apache.storm.Config;
import org.apache.storm.Constants;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Map;

public class HashtagLoggerBolt implements IRichBolt {
    private OutputCollector collector;
    private Map<String, LossyHashtag> hashtagCounts;
    private BufferedWriter writer;
    private String logLocation;

    public HashtagLoggerBolt() {
        this.logLocation = "/s/chopin/b/grad/millerr/storm_logs/log.txt";
    }

    @Override
    public void prepare(Map<String, Object> map, TopologyContext topologyContext, OutputCollector outputCollector) {
        try {
            this.writer = new BufferedWriter(new FileWriter(logLocation, true));
        } catch (IOException e) {
            e.printStackTrace();
        }
        this.hashtagCounts = new HashMap<String, LossyHashtag>();
        this.collector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
        if (isTickTuple(tuple) && hashtagCounts.size() != 0) {
            //Write out 100 entries with the same timestamp every 10 seconds
            Map<String, LossyHashtag> sortedCounts = LossyHashtag.sortByValues(hashtagCounts);
            int iterations = 0;
            String currentTime = new Timestamp(new java.util.Date().getTime()).toString();
            try {
                writer.write("<" + currentTime + ">");
                for (Map.Entry<String, LossyHashtag> e : sortedCounts.entrySet()) {
                    if (iterations < 100) {
                        writer.write(e.getValue().toLogEntry());
                    }
                    iterations++;
                }
                writer.write("\n");
                writer.flush();
            } catch (Exception e) {
                e.printStackTrace();
            }

            System.out.println("Wrote a batch at: " + currentTime);
        } else {
            //If not tick tuple, add entry to buffer for write later
            if (tuple.getFields().contains("hashtag")) {
                LossyHashtag item = (LossyHashtag) tuple.getValueByField("hashtag");
                hashtagCounts.put(item.getHashtag(), item);
            } else {
                System.out.println("----TICK MESSAGE----");
            }
        }
    }

    @Override
    public void cleanup() {
        try {
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }

    private static boolean isTickTuple(Tuple tuple) {
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
