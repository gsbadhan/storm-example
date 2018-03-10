package com.storm.buysellsentiments;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.utils.Utils;

public class StockBuySellSentimentTopology {

	public static void main(String[] args) {
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("StockBuySellSpout", new StockBuySellSpout());
		builder.setBolt("StockBuySellBolt", new StockBuySellBolt(), 4).fieldsGrouping("StockBuySellSpout",
				new Fields("stockName"));
		
		Config config = new Config();
		LocalCluster localCluster = new LocalCluster();
		localCluster.submitTopology("StockBuySellSentimentTopology", config, builder.createTopology());
		Utils.sleep(60 * 1000);
		localCluster.killTopology("StockBuySellSentimentTopology");
		localCluster.shutdown();
	}

}
