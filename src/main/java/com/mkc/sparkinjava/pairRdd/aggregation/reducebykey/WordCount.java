package com.mkc.sparkinjava.pairRdd.aggregation.reducebykey;

import java.util.Arrays;
import java.util.Map;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class WordCount {

	public static void main(String[] args) {
		
		Logger.getLogger("org").setLevel(Level.ERROR);
		SparkConf conf = new SparkConf().setAppName("wordCounts").setMaster("local[3]");
		JavaSparkContext sc = new JavaSparkContext(conf);

		JavaRDD<String> lines = sc.textFile("src/in/word_count.txt");
		JavaRDD<String> words = lines.flatMap(line -> Arrays.asList(line.split(" ")).iterator());
		JavaPairRDD<String, Integer> wordPairRdd = words.mapToPair((PairFunction<String, String, Integer>) word -> new Tuple2<>(word, 1));
		JavaPairRDD<String, Integer> wordCount = wordPairRdd.reduceByKey((Function2<Integer, Integer, Integer>) (x,y) -> x+y);
		Map<String, Integer> wordCountMap = wordCount.collectAsMap();
		
		for (Map.Entry<String, Integer> entry : wordCountMap.entrySet()) {
			System.out.println(entry.getKey() + " : " + entry.getValue());
		}
		sc.close();
	}

}
