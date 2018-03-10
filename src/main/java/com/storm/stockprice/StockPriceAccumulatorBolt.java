package com.storm.stockprice;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class StockPriceAccumulatorBolt extends BaseRichBolt {
	private OutputCollector collector;
	private ConcurrentHashMap<String, Float> stockPriceMap = new ConcurrentHashMap<>();

	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		// push latest stock price to next bolt after every 3 seconds
		Executors.newScheduledThreadPool(1).scheduleWithFixedDelay(new Runnable() {
			@Override
			public void run() {
				stockPriceMap.forEach((k, v) -> collector.emit(new Values(k, stockPriceMap.remove(k))));
			}
		}, 1, 3, TimeUnit.SECONDS);
	}

	@Override
	public void execute(Tuple input) {
		String stock = input.getStringByField("stockName");
		Float price = input.getFloatByField("price");
		stockPriceMap.put(stock, price);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("stock", "price"));
	}

}
