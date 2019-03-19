package com.mkc.sparkinjava.pairRdd.groupbykey;

import java.util.Map;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.mkc.sparkinjava.utils.Utils;

import scala.Tuple2;

public class AirportsByCountrySolution {

	public static void main(String[] args) {
		Logger.getLogger("org").setLevel(Level.ERROR);
		SparkConf conf = new SparkConf().setAppName("airports").setMaster("local[*]");
		JavaSparkContext sc = new JavaSparkContext(conf);

		JavaRDD<String> lines = sc.textFile("src/in/airport.txt");
		JavaPairRDD<String, String> countryAndAirportNameAndPair = lines.mapToPair(
				record -> new Tuple2<>(record.split(Utils.COMMA_DELIMITER)[3], record.split(Utils.COMMA_DELIMITER)[1]));

		JavaPairRDD<String, Iterable<String>> airportByCountry = countryAndAirportNameAndPair.groupByKey();

		for (Map.Entry<String, Iterable<String>> airports : airportByCountry.collectAsMap().entrySet()) {
			System.out.println(airports.getKey() + " : " + airports.getValue());
		}

		sc.close();
	}

}
