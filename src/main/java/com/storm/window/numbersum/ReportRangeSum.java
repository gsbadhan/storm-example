package com.storm.window.numbersum;

import java.util.HashMap;
import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

public class ReportRangeSum extends BaseRichBolt {
	private OutputCollector collector;
	private Map<String, Integer> rangeSumMap = new HashMap<>();

	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
	}

	@Override
	public void execute(Tuple input) {
		String range = input.getStringByField("range");
		Integer sum = input.getIntegerByField("sum");
		rangeSumMap.put(range, sum);
		rangeSumMap.forEach((k, v) -> System.out.print(k + ":" + v + " "));
		System.out.println();
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// NOOP
	}

}
