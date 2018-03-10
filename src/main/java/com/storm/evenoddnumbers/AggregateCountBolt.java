package com.storm.evenoddnumbers;

import java.util.HashMap;
import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

public class AggregateCountBolt extends BaseRichBolt {
	private OutputCollector collector;
	private Map<String, Integer> countMap = new HashMap<>();
	private int globalCounter = 0;

	@Override
	public void prepare(Map map, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
	}

	@Override
	public void execute(Tuple tuple) {
		String type = tuple.getStringByField("type");
		Integer count = tuple.getIntegerByField("count");
		countMap.put(type, count);
		System.out.println(Thread.currentThread() + ":" + this.hashCode() + ": global count:" + (++globalCounter) + ":"
				+ countMap);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	}

}
