package com.mkc.sparkinjava.pairRdd.aggregation.reducebykey.housePrice;

import java.io.Serializable;

public class AvgCount implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	private int count;
	private double total;

	public AvgCount(int count, double total) {
		this.count = count;
		this.total = total;
	}

	public int getCount() {
		return count;
	}

	public double getTotal() {
		return total;
	}

	@Override
	public String toString() {
		return "AvgCount{" + "count=" + count + ", total=" + total + '}';
	}
}