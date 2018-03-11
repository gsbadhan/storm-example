package com.storm.productsclicked;

import java.util.Map;
import java.util.Random;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

public class ProductsClickedEventSpout extends BaseRichSpout {
	private SpoutOutputCollector collector;
	private Random randomProduct = new Random(9);

	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		this.collector = collector;
	}

	@Override
	public void nextTuple() {
		String user = StaticTestData.USERS[Math.abs(randomProduct.nextInt() % (StaticTestData.USERS.length - 1))];
		String site = StaticTestData.SITES[Math.abs(randomProduct.nextInt() % (StaticTestData.SITES.length - 1))];
		String product = StaticTestData.PRODUCTS[Math.abs(randomProduct.nextInt() % (StaticTestData.PRODUCTS.length - 1))];
		collector.emit(new Values(user, site, product));
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("userId", "site_url", "product"));
	}

}
