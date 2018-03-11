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
			return (int) ((buySell.buyCount / buySell.getTotal()) * 100);
		} else {
			return (int) ((buySell.sellCount / buySell.getTotal()) * 100);
		}
	}

	@Override
	public String toString() {
		return "[buy=" + percentage(true) + "%, sell=" + percentage(false) + "%]";
	}

	public static class BuySell {
		private float buyCount;
		private float sellCount;

		public BuySell(float buyCount, float sellCount) {
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

		public float getTotal() {
			float totl = buyCount + sellCount;
			return totl <= 0 ? 1 : totl;
		}

	}

}
