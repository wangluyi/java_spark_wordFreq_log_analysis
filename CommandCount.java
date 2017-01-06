/*
 * (c) University of Zurich 2016
 * author: Luyi Wang
 * date: 2016.12
 */

package assignment3;

import scala.Tuple2;
import scala.language;

import org.apache.cassandra.thrift.Cassandra.system_add_column_family_args;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import breeze.linalg.min;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class CommandCount {
	public static void main(String[] args) throws Exception {

		SparkConf sparkConf = new SparkConf().setAppName("JavaCommandCount").setMaster("local");
		JavaSparkContext ctx = new JavaSparkContext(sparkConf);
		JavaRDD<String> lines = ctx.textFile(args[0], 1).cache();
				
		// flatmap: line with command -> timestamp of command 
		JavaRDD<Long> cmd_timestamp = lines.flatMap(new FlatMapFunction<String, Long>() {
			@Override
			public Iterable<Long> call(String s){
				if (s.contains("<Command ")) {
					Pattern p = Pattern.compile("timestamp=\"(\\d*?)\"");
					Matcher m = p.matcher(s);
					if (m.find()) {
					    return Arrays.asList(Long.parseLong(m.group(1)));
					}
				}
				return Arrays.asList();  // must not return null 
			}
		});
		
		// long -> (time_interval, 1)
		JavaPairRDD<Integer, Integer> interval_one = cmd_timestamp.mapToPair(new PairFunction<Long, Integer, Integer>() {
			@Override
			public Tuple2<Integer, Integer> call(Long a) {
				int interval = (int)(a/1000/(15*60));
				return new Tuple2<Integer, Integer>(interval, 1);
			}
		});
		
		// (time_interval, 1) -> (time_interval, count)
		JavaPairRDD<Integer, Integer> interval_count = interval_one.reduceByKey(new Function2<Integer,Integer,Integer>(){
			@Override
			public Integer call(Integer a, Integer b){
				return (a+b);
			}
		});
		
		JavaPairRDD<Integer, Integer> intervalPairSorted = interval_count.sortByKey(true);
		List<Tuple2<Integer, Integer>> output = intervalPairSorted.collect();

		File file = new File(args[1]);
		BufferedWriter outputBW = new BufferedWriter(new FileWriter(file));

		for (Tuple2<?, ?> tuple : output) {
			System.out.println(tuple._1() + "\t" + tuple._2());
			outputBW.write(tuple._1() + "\t" + tuple._2() +"\n");
		}
		outputBW.close();
		ctx.stop();
	}
}
