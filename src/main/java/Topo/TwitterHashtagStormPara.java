package Topo;

import Spout.*;
import Bolt.*;


import org.apache.storm.StormSubmitter;
import org.apache.storm.tuple.Fields;
import org.apache.storm.LocalCluster;
import org.apache.storm.Config;
import org.apache.storm.topology.TopologyBuilder;

public class TwitterHashtagStormPara {
    public static void main(String[] args) throws Exception {
        String consumerKey = "A8nzDPkk2T17MkRpUEN0YiBS6";
        String consumerSecret = "NqvdfcLuc5O6d8oxeonWDgScvYSn5KrjGeZtdbjiwCQTyqH2vD";

        String accessToken = "1232742956160864258-owzqWwtlZ8iov6VYOb80nFJzdbQCkg";
        String accessTokenSecret = "y2jWZ0TE2wNW1YFDmzORNCBprUpsDVDrTRav708ehebTk";

        Config config = new Config();
        config.setDebug(false);
        config.setNumWorkers(4);

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("twitter-spout", new TwitterSampleSpout(consumerKey,
                consumerSecret, accessToken, accessTokenSecret));

        builder.setBolt("twitter-hashtag-reader-bolt", new HashtagReaderBolt())
                .shuffleGrouping("twitter-spout");

        builder.setBolt("twitter-hashtag-counter-bolt", new HashtagCounterBolt(0.005, 0.1), 4)
                .fieldsGrouping("twitter-hashtag-reader-bolt", new Fields("hashtag"));

        builder.setBolt("twitter-hashtag-logger-bolt", new HashtagLoggerBolt())
                .globalGrouping("twitter-hashtag-counter-bolt");

//        LocalCluster cluster = new LocalCluster();
//        cluster.submitTopology("TwitterHashtagStorm", config,
//                builder.createTopology());

        try {
            StormSubmitter.submitTopology("twitter_lossy_count_para", config, builder.createTopology());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}