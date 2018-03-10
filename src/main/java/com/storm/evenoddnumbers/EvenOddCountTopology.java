package com.storm.evenoddnumbers;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.utils.Utils;

public class EvenOddCountTopology {

	public static void main(String[] args) {
		TopologyBuilder topologyBuilder = new TopologyBuilder();
		topologyBuilder.setSpout("EvenOddSpout", new EvenOddSpout(), 1);
		topologyBuilder.setBolt("EvenOddSplitterBolt", new EvenOddSplitterBolt(), 2).shuffleGrouping("EvenOddSpout");
		topologyBuilder.setBolt("EvenOddGroupCountBolt", new EvenOddGroupCountBolt(), 5)
				.fieldsGrouping("EvenOddSplitterBolt", new Fields("type"));
		topologyBuilder.setBolt("AggregateCountBolt", new AggregateCountBolt()).globalGrouping("EvenOddGroupCountBolt");

		Config config = new Config();

		LocalCluster localCluster = new LocalCluster();
		localCluster.submitTopology("EvenOddCountTopology", config, topologyBuilder.createTopology());
		Utils.sleep(30 * 1000);
		localCluster.killTopology("EvenOddCountTopology");
		localCluster.shutdown();
	}
}
