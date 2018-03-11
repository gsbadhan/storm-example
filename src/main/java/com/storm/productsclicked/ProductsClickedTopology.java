package com.storm.productsclicked;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.utils.Utils;

public class ProductsClickedTopology {

	public static void main(String[] args) {
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("ProductsClickedEventSpout", new ProductsClickedEventSpout());
		builder.setBolt("ProductsClickedEventBolt", new ProductsClickedEventBolt(), 10)
				.fieldsGrouping("ProductsClickedEventSpout", new Fields("userId"));
		
		Config config = new Config();
		LocalCluster localCluster = new LocalCluster();
		localCluster.submitTopology("ProductsClickedTopology", config, builder.createTopology());
		Utils.sleep(120 * 1000);
		localCluster.killTopology("ProductsClickedTopology");
		localCluster.shutdown();
	}

}
