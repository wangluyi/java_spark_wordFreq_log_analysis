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

public final class CommandFrequency {
	public static void main(String[] args) throws Exception {

		SparkConf sparkConf = new SparkConf().setAppName("JavaCommandFrequency").setMaster("local");
		JavaSparkContext ctx = new JavaSparkContext(sparkConf);
		JavaRDD<String> lines = ctx.textFile(args[0], 1).cache();
				
		JavaRDD<String> cmd_type = lines.flatMap(new FlatMapFunction<String, String>() {
			@Override
			public Iterable<String> call(String s){
				String type = "";
				if (s.contains("<Command ")) { // this line is a command 
					Pattern p1 = Pattern.compile("_type=\"(.*?)\"");  //? means not greedy 
					Matcher m1 = p1.matcher(s);
					if (m1.find()==true) {
						type = m1.group(1);
					}
					if (type.equals("EclipseCommand")!=true) {
						return Arrays.asList(type);
					} else {
						Pattern p2 = Pattern.compile("commandID=\"(.*?)\"");  //not greedy 
						Matcher m2 = p2.matcher(s);
						if (m2.find()==true) {
							type = "EclipseCommand:"+m2.group(1);
							return Arrays.asList(type);
						}
					}				    
				}

				return Arrays.asList();  // must not return null! 
			}
		});

		// map: type_string -> (type, 1)
		JavaPairRDD<String, Integer> cmdtype_one = cmd_type.mapToPair(new PairFunction<String, String, Integer>() {
			@Override
			public Tuple2<String, Integer> call(String a) {
				return new Tuple2<String, Integer>(a, 1);
			}
		});
		
		// reduce (cmdtype, 1) -> (cmdtype, count)
		JavaPairRDD<String, Integer> cmdtype_count = cmdtype_one.reduceByKey(new Function2<Integer,Integer,Integer>(){
			@Override
			public Integer call(Integer a, Integer b){
				return (a+b);
			}
		});
		
		// swap, sort by key, swap back 
		JavaPairRDD<Integer, String> swappedPair = cmdtype_count
				.mapToPair(new PairFunction<Tuple2<String, Integer>, Integer, String>() {
					@Override
					public Tuple2<Integer, String> call(Tuple2<String, Integer> item) throws Exception {
						return item.swap();
					}
				});
		JavaPairRDD<Integer, String> swappedPairSorted = swappedPair.sortByKey(false);
		JavaPairRDD<String, Integer> swappedPairSortedSwapBack = swappedPairSorted
				.mapToPair(new PairFunction<Tuple2<Integer, String>, String, Integer>() {
					@Override
					public Tuple2<String, Integer> call(Tuple2<Integer, String> item) throws Exception {
						return item.swap();
					}
				});

		List<Tuple2<String, Integer>> output = swappedPairSortedSwapBack.collect(); 
		
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
