package com.storm.buysellsentiments;

import java.util.Map;
import java.util.Random;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

public class StockBuySellSpout extends BaseRichSpout {
	private SpoutOutputCollector collector;
	private Random priceGenerator = new Random(10);
	private String[] stocks = { "cpa", "newX", "micr", "mng" };

	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		this.collector = collector;
	}

	@Override
	public void nextTuple() {
		for (int i = 0; i < stocks.length; i++) {
			int num = priceGenerator.nextInt();
			if (num % 2 == 0)
				if (num % 3 == 0)
					this.collector.emit(new Values(stocks[i], "buy", "yes"));
				else
					this.collector.emit(new Values(stocks[i], "buy", "no"));
			else if (num % 3 == 0)
				this.collector.emit(new Values(stocks[i], "sell", "yes"));
			else
				this.collector.emit(new Values(stocks[i], "sell", "no"));
			Utils.sleep(1);
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("stockName", "buySell", "yesNo"));
	}

}
