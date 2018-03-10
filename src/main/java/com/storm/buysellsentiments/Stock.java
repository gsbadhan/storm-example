package com.storm.buysellsentiments;

public class Stock {
	private BuySell buySell;

	public Stock(BuySell buySell) {
		super();
		this.buySell = buySell;
	}

	public BuySell getBuySell() {
		return buySell;
	}

	private int percentage(boolean isBuy) {
		if (isBuy) {
			return (buySell.buyCount / buySell.getTotal()) * 100;
		} else {
			return (buySell.sellCount / buySell.getTotal()) * 100;
		}
	}

	@Override
	public String toString() {
		return "[buy=" + percentage(true) + "%, sell=" + percentage(false) + "%]";
	}

	public static class BuySell {
		private int buyCount;
		private int sellCount;

		public BuySell(int buyCount, int sellCount) {
			super();
			this.buyCount = buyCount;
			this.sellCount = sellCount;
		}

		public void incrBuyCount() {
			++this.buyCount;
		}

		public void dcrBuyCount() {
			if (this.buyCount > 0)
				--this.buyCount;
		}

		public void incrSellCount() {
			++this.sellCount;
		}

		public void dcrSellCount() {
			if (this.sellCount > 0)
				--this.sellCount;
		}

		public int getTotal() {
			int totl = buyCount + sellCount;
			return totl <= 0 ? 1 : totl;
		}

	}

}
