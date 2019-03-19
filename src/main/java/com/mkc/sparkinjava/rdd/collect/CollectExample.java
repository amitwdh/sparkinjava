package com.mkc.sparkinjava.rdd.collect;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class CollectExample {

	public static void main(String[] args) {

		SparkConf conf = new SparkConf().setAppName("count").setMaster("local[*]");
		JavaSparkContext sc = new JavaSparkContext(conf);

		List<String> inputWords = Arrays.asList("spark", "hadoop", "spark", "hive", "pig", "cassandra", "hadoop");
		JavaRDD<String> wordRdd = sc.parallelize(inputWords);
		List<String> words = wordRdd.collect();
		
		words.forEach(System.out::println);
		sc.close();
	}

}
