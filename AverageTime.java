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

public final class AverageTime {
	public static void main(String[] args) throws Exception {

		SparkConf sparkConf = new SparkConf().setAppName("JavaAverageTime").setMaster("local");
		JavaSparkContext ctx = new JavaSparkContext(sparkConf);
		
		JavaRDD<String> lines = ctx.textFile(args[0], 1).cache();

		// count the commands 
		long num_command = lines.filter(new Function<String, Boolean>() {
			@Override
			public Boolean call(String s) throws Exception {
				return s.contains("<Command "); 
			}
		}).count();
				
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
					else {
						return Arrays.asList();  // must not return null
					}
				}
				return Arrays.asList();  // must not return null 
			}
		});

		// timestamp -> (1, timestamp), keep the same key 
		JavaPairRDD<Integer, Long> one_timestp = cmd_timestamp.mapToPair(new PairFunction<Long, Integer, Long>() {
			@Override
			public Tuple2<Integer, Long> call(Long a) {
				return new Tuple2<Integer, Long>(1, a);
			}
		});
		
		// redube by key, find the max timestamp 
		JavaPairRDD<Integer, Long> max_timestp = one_timestp.reduceByKey(new Function2<Long,Long,Long>(){
			@Override
			public Long call(Long a, Long b){
				if (a > b) {
					return a;
				} else {
					return b;
				}
			}
		});
		
		// redube by key, find the min timestamp 
		JavaPairRDD<Integer, Long> min_timestp = one_timestp.reduceByKey(new Function2<Long,Long,Long>(){
			@Override
			public Long call(Long a, Long b){
				if (a < b) {
					return a;
				} else {
					return b;
				}
			}
		});

		List<Tuple2<Integer, Long>> max_t = max_timestp.take(1);
		List<Tuple2<Integer, Long>> min_t = min_timestp.take(1);
		long timestamp_diff = max_t.get(0)._2() - min_t.get(0)._2();

		File file = new File(args[1]);
		BufferedWriter outputBW = new BufferedWriter(new FileWriter(file));

		System.out.println(String.format("%.2f", (float)timestamp_diff/(num_command - 1)));
		outputBW.write(String.format("%.2f", (float)timestamp_diff/(num_command - 1))); 
		outputBW.close();
		ctx.stop();
	}
}
