package com.mkc.sparkinjava.rdd.airports;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.mkc.sparkinjava.utils.Utils;

public class AirportsByLatitudeSolution {

	public static void main(String[] args) {
		SparkConf config = new SparkConf().setMaster("local[2]").setAppName("AirportsInUSA");
		JavaSparkContext sc = new JavaSparkContext(config);
		JavaRDD<String> airports = sc.textFile("src/in/airport.txt");
		JavaRDD<String> airportsLatitudeMoreThan40 = airports.filter(airport -> Float.valueOf(airport.split(Utils.COMMA_DELIMITER)[6]) > 40);
		JavaRDD<String> airportNameAndLatitude = airportsLatitudeMoreThan40.map(record -> {
			String []split = record.split(Utils.COMMA_DELIMITER);
			return StringUtils.join(new String [] {split[1], split[6]}, ",");
		});
		airportNameAndLatitude.saveAsTextFile("src/out/airports_by_latitude.text");
		sc.close();
	}

}
