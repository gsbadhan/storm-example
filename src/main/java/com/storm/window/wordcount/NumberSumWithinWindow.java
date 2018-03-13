package com.storm.window.wordcount;

import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.windowing.TupleWindow;

public class NumberSumWithinWindow extends BaseWindowedBolt {
	private OutputCollector collector;

	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
	}

	@Override
	public void execute(TupleWindow inputWindow) {
		int sum = 0;
		int x = inputWindow.get().get(0).getIntegerByField("number");
		int y = inputWindow.get().get(inputWindow.get().size() - 1).getIntegerByField("number");
		for (int i = 0; i < inputWindow.get().size(); i++) {
			sum = sum + inputWindow.get().get(i).getIntegerByField("number");
		}
		collector.emit(new Values((x + "-" + y), sum));
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("range", "sum"));
	}
}
