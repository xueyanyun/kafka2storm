package com.eb.bi.etl.kafka2storm;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.spout.Scheme;
import backtype.storm.utils.Utils;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import storm.kafka.BrokerHosts;
import storm.kafka.ZkHosts;
import storm.kafka.StringScheme;
import storm.kafka.SpoutConfig;
import storm.kafka.KafkaSpout;
import storm.kafka.bolt.KafkaBolt;

import org.apache.storm.hbase.bolt.HBaseBolt;
import org.apache.storm.hbase.bolt.mapper.HBaseMapper;
import org.apache.storm.hbase.bolt.mapper.SimpleHBaseMapper;

import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;

public class Kafka2StormTopo {

	public static void main(String[] args) throws Exception {
		if (args != null && args.length < 2) {
			System.out.println("Usage: storm jar xxx.jar zkHost:zkPort topic");
			System.exit(1);
		}

		String zkCfg = args[0];
		String topic = args[1];
		if (args.length == 2) {
			zkCfg = args[0];
			topic = args[1];
		} else {
			zkCfg = args[1];
			topic = args[2];
		}

		BrokerHosts brokerHosts = new ZkHosts(zkCfg);
		SpoutConfig spoutConfig = new SpoutConfig(brokerHosts, topic, "/ebupt", "kafkaspout");
		Config conf = new Config();
		//Map<String, String> map = new HashMap<String, String>() {{
		//		put("metadata.broker.list", "eb179:9092");
		//		put("serializer.class", "kafka.serializer.StringEncoder");
		//	}};
		//conf.put("kafka.broker.properties", map);
		//conf.put("topic", "test2");
		//Map<String, Object> map = new HashMap<String, Object>();
		//conf.put("hbase.config", map);

		spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
		//spoutConfig.zkServers = new ArrayList<String>() {{add("10.1.69.179");}};
		//spoutConfig.zkPort = 2181;
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("spout", new KafkaSpout(spoutConfig));
		builder.setBolt("bolt", new SequenceBolt()).shuffleGrouping("spout");
		//builder.setBolt("kafkabolt", new KafkaBolt<Integer, String>()).shuffleGrouping("bolt");

		//HBaseMapper mapper = new SimpleHBaseMapper().withRowKeyField("key").withColumnFields(new Fields("key")).withColumnFamily("cf");
		//builder.setBolt("hbasebolt", new HBaseBolt("storm2hbase", mapper).withConfigKey("hbase.config")).fieldsGrouping("bolt", new Fields("key"));

		if (args != null && args.length > 2) {
			conf.setNumWorkers(3);
			StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
		} else {
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("Topo", conf, builder.createTopology());
			Utils.sleep(100000);
			cluster.killTopology("Topo");
			cluster.shutdown();
		}
	}
}

