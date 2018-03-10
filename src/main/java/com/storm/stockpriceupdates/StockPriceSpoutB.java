package com.storm.stockpriceupdates;

import java.util.Map;
import java.util.Random;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

public class StockPriceSpoutB extends BaseRichSpout {
	private SpoutOutputCollector collector;
	private Random priceGenerator = new Random(10);
	private String[] stocks = { "mgm", "yc", "abb", "tyc", "royl" };

	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		this.collector = collector;
	}

	@Override
	public void nextTuple() {
		for (int i = 0; i < stocks.length; i++) {
			this.collector.emit(new Values(stocks[i], priceGenerator.nextFloat()));
			Utils.sleep(500);
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("stockName", "price"));
	}

}
