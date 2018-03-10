package com.storm.evenoddnumbers;

import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class EvenOddSplitterBolt extends BaseRichBolt {
	private OutputCollector collector;

	@Override
	public void prepare(Map map, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
	}

	@Override
	public void execute(Tuple tuple) {
		int num = tuple.getIntegerByField("raw_number");
		if (num % 2 == 0) {
			collector.emit(new Values("even", num));
		} else {
			collector.emit(new Values("odd", num));
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("type", "number"));
	}

}
