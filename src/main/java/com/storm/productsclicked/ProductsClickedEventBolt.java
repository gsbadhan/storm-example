package com.storm.productsclicked;

import java.util.HashMap;
import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import com.datastax.driver.core.ResultSet;
import com.storm.cassandra.CassandraConnector;

public class ProductsClickedEventBolt extends BaseRichBolt {
	private OutputCollector collector;
	private CassandraConnector connector;

	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		this.connector = new CassandraConnector("localhost", 9042);
	}

	@Override
	public void execute(Tuple input) {
		String userId = input.getStringByField("userId");
		String siteUrl = input.getStringByField("site_url");
		String product = input.getStringByField("product");
		userProductClickProfiling(userId, siteUrl, product);
	}

	private void userProductClickProfiling(String userId, String siteUrl, String product) {
		Map<String, Object> params = new HashMap<>(3);
		params.put("user_id", userId);
		params.put("site_url", siteUrl);
		params.put("product", product);
		connector.getSession().execute(
				"update products.user_product_profiling  set clicked=clicked + 1 where user_id=:user_id and site_url=:site_url and product=:product", params);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
	}

}
