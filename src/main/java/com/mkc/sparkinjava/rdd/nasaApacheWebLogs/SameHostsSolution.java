package com.mkc.sparkinjava.rdd.nasaApacheWebLogs;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class SameHostsSolution {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("SameHostsSolution").setMaster("local[1]");
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaRDD<String> julyFirstLogs = sc.textFile("src/in/nasa_19950701.tsv");
		JavaRDD<String> augustFirstLogs = sc.textFile("src/in/nasa_19950801.tsv");
		JavaRDD<String> julyFirstHost = julyFirstLogs.map(record -> record.split("\t")[0]);
		JavaRDD<String> augustFirstHost = augustFirstLogs.map(record -> record.split("\t")[0]);
		JavaRDD<String> intersection = julyFirstHost.intersection(augustFirstHost);
		JavaRDD<String> filterIntersection = intersection.filter(record -> !record.equals("host"));
		filterIntersection.saveAsTextFile("src/out/nasa_logs_same_hosts.csv");
		sc.close();
	}

}
