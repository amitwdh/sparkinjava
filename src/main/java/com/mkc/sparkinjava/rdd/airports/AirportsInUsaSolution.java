package com.mkc.sparkinjava.rdd.airports;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.mkc.sparkinjava.utils.Utils;

public class AirportsInUsaSolution {

	public static void main(String[] args) throws InterruptedException {

		SparkConf config = new SparkConf().setMaster("local[2]").setAppName("AirportsInUSA");
		JavaSparkContext sc = new JavaSparkContext(config);
		JavaRDD<String> airports = sc.textFile("src/in/airport.txt");
		JavaRDD<String> airportsInUSA = airports.filter(airport -> airport.split(Utils.COMMA_DELIMITER)[3].equals("\"United States\""));
		JavaRDD<String> airportNameAndCityNames = airportsInUSA.map(record -> {
			String[] columns = record.split(",");
			return StringUtils.join(new String[] { columns[1], columns[2] }, ",");
		});
		airportNameAndCityNames.saveAsTextFile("src/out/airport_in_usa.text");
		sc.close();
	}

}
