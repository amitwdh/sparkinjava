package com.mkc.sparkinjava.rdd.reduce;

import java.util.Arrays;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class SumOfNumbersSolution {

	public static void main(String []args) {
		
		Logger.getLogger("org").setLevel(Level.OFF);
		SparkConf conf = new SparkConf().setAppName("reduce").setMaster("local[*]");
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaRDD<String> lines = sc.textFile("src/in/prime_nums.text");
		JavaRDD<String> numbers = lines.flatMap(record -> Arrays.asList(record.split("\\s+")).iterator());
		JavaRDD<String> validNumbers = numbers.filter(number -> !number.isEmpty());
		JavaRDD<Integer> validIntegers = validNumbers.map(record -> Integer.valueOf(record));
		Integer totalSum = validIntegers.reduce( (x,y) -> x + y );
		System.out.println("Sum is: "+totalSum);
		sc.close();
	}
}
