package com.storm.evenoddnumbers;

import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class EvenOddGroupCountBolt extends BaseRichBolt {
	private OutputCollector collector;
	private int count = 0;

	@Override
	public void prepare(Map map, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
	}

	@Override
	public void execute(Tuple tuple) {
		String type = tuple.getStringByField("type");
		Integer num = tuple.getIntegerByField("number");
		if (type.equalsIgnoreCase("even")) {
			collector.emit(new Values("even", num, ++count));
		} else if (type.equalsIgnoreCase("odd")) {
			collector.emit(new Values("odd", num, ++count));
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("type", "number", "count"));
	}

}
