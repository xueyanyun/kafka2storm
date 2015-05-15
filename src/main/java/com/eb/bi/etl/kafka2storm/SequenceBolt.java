package com.eb.bi.etl.kafka2storm;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.task.TopologyContext;

import java.util.Map;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

public class SequenceBolt extends BaseBasicBolt {

	//BufferedWriter bw = null;

	//@Override
	//public void prepare(Map conf, TopologyContext context) {
	//	super.prepare(conf, context);
	//	try {
	//		bw = new BufferedWriter(new FileWriter("kafka-msg.log"));
	//	} catch (IOException e) {
	//		
	//	}
	//}

	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		String word = (String) input.getValue(0);
		//String out = "I'm " + word + "!";
		//collector.emit(new Values(out));
		System.out.println("RECEIVE A NEW WORD: " + word);
		//try {
		//	bw.append(word + "\n");
		//} catch (IOException e) {
		//	
		//}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		//declarer.declare(new Fields("key"));
	}

	//@Override
	//public void cleanup() {
	//	try {
	//		bw.close();
	//	} catch (IOException e) {
	//		
	//	}
	//}
}
