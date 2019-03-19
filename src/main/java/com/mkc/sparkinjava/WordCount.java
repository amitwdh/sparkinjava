package com.mkc.sparkinjava;

import java.util.Arrays;
import java.util.Map;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class WordCount {

	public static void main(String[] args) {
		
		Logger.getLogger("org").setLevel(Level.ERROR);
		SparkConf conf = new SparkConf().setAppName("wordCounts").setMaster("local[3]");
		JavaSparkContext sc = new JavaSparkContext(conf);

		JavaRDD<String> lines = sc.textFile("src/in/word_count.txt");
		JavaRDD<String> words = lines.flatMap(line -> Arrays.asList(line.split(" ")).iterator());

		Map<String, Long> wordCounts = words.countByValue();

		for (Map.Entry<String, Long> entry : wordCounts.entrySet()) {
			System.out.println(entry.getKey() + " : " + entry.getValue());
		}
		sc.close();
	}

}
