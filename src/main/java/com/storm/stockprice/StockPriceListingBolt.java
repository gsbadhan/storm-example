package com.storm.stockprice;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

public class StockPriceListingBolt extends BaseRichBolt {
	private OutputCollector collector;
	private ConcurrentHashMap<String, Float> stockListing = new ConcurrentHashMap<>();

	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		//show stock list after every 5 seconds
		Executors.newScheduledThreadPool(1).scheduleAtFixedRate(new Runnable() {
			@Override
			public void run() {
				System.out.println("Stock List:" + stockListing);
			}
		}, 5, 5, TimeUnit.SECONDS);
	}

	@Override
	public void execute(Tuple input) {
		String stock = input.getStringByField("stock");
		Float price = input.getFloatByField("price");
		stockListing.put(stock, price);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
	}

}
