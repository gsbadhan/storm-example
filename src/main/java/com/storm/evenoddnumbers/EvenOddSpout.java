package com.storm.evenoddnumbers;

import java.util.Map;
import java.util.Random;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

public class EvenOddSpout extends BaseRichSpout {
	private SpoutOutputCollector outputCollector;
	private Random randomNumGenerator=new Random(1);
	@Override
	public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector collector) {
		this.outputCollector = collector;
	}

	@Override
	public void nextTuple() {
		this.outputCollector.emit(new Values(new Integer(randomNumGenerator.nextInt())));
		Utils.sleep(1000);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("raw_number"));
	}

}
