package com.storm.window.numbersum;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseWindowedBolt.Count;
import org.apache.storm.tuple.Fields;
import org.apache.storm.utils.Utils;

public class NumberSumTopology {

	public static void main(String args[]) {
		final int SUM_WINDOW = 5;
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("GenerateNumbersSpout", new GenerateNumbersSpout(), 1);
		builder.setBolt("NumberSumWithinWindow",
				new NumberSumWithinWindow().withWindow(new Count(SUM_WINDOW), new Count(SUM_WINDOW)), 1)
				.fieldsGrouping("GenerateNumbersSpout", new Fields("number"));
		builder.setBolt("ReportRangeSum", new ReportRangeSum()).globalGrouping("NumberSumWithinWindow");

		Config config = new Config();
		LocalCluster localCluster = new LocalCluster();
		localCluster.submitTopology("NumberSumTopology", config, builder.createTopology());
		Utils.sleep(60 * 1000);
		localCluster.killTopology("NumberSumTopology");
		localCluster.shutdown();
	}
}
