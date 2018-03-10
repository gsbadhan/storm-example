package com.storm.buysellsentiments;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import com.storm.buysellsentiments.Stock.BuySell;

public class StockBuySellBolt extends BaseRichBolt {
	private OutputCollector collector;
	private ConcurrentHashMap<String, Stock> stockSentimentMap = new ConcurrentHashMap<>();

	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		Executors.newScheduledThreadPool(1).scheduleWithFixedDelay(new Runnable() {
			@Override
			public void run() {
				if (!stockSentimentMap.isEmpty())
					System.out.println("Stock Sentiments:" + stockSentimentMap);
			}
		}, 5, 5, TimeUnit.SECONDS);
	}

	@Override
	public void execute(Tuple input) {
		String stock = input.getStringByField("stockName");
		String buyOrSell = input.getStringByField("buySell");
		String yesNo = input.getStringByField("yesNo");
		buyOrSell(stock, buyOrSell, yesNo);

	}

	private void buyOrSell(String stock, String buyOrSell, String yesNo) {
		if (!stockSentimentMap.containsKey(stock))
			stockSentimentMap.put(stock, new Stock(new BuySell(0, 0)));

		if (buyOrSell.equalsIgnoreCase("buy")) {
			if (yesNo.equalsIgnoreCase("yes"))
				stockSentimentMap.get(stock).getBuySell().incrBuyCount();
			else
				stockSentimentMap.get(stock).getBuySell().dcrBuyCount();
		} else if (buyOrSell.equalsIgnoreCase("sell")) {
			if (yesNo.equalsIgnoreCase("yes"))
				stockSentimentMap.get(stock).getBuySell().incrSellCount();
			else
				stockSentimentMap.get(stock).getBuySell().dcrSellCount();
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
	}

}
