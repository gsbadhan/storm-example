package com.storm.stockpriceupdates;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.utils.Utils;

public class StockPriceTopology {

	public static void main(String[] args) {

		/**
		 * I used 2 spouts here to simulate 2 input channels
		 */
		TopologyBuilder builder = new TopologyBuilder();

		builder.setSpout("StockPriceSpoutA", new StockPriceSpoutA(), 2);
		builder.setSpout("StockPriceSpoutB", new StockPriceSpoutB(), 2);
		builder.setBolt("StockPriceAccumulatorBolt", new StockPriceAccumulatorBolt(), 5)
				.fieldsGrouping("StockPriceSpoutA", new Fields("stockName"))
				.fieldsGrouping("StockPriceSpoutB", new Fields("stockName"));
		builder.setBolt("stockList", new StockPriceListingBolt()).globalGrouping("StockPriceAccumulatorBolt");

		Config config = new Config();
		LocalCluster localCluster = new LocalCluster();
		localCluster.submitTopology("StockPriceTopology", config, builder.createTopology());
		Utils.sleep(60 * 1000);
		localCluster.killTopology("StockPriceTopology");
		localCluster.shutdown();
	}
}
