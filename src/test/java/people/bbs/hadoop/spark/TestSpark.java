package people.bbs.hadoop.spark;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class TestSpark {

	public static void main(String[] args) {
		// 设置spark master节点地址，app名以及部署方式
		SparkConf conf = new SparkConf();
	     conf.set("spark.testing.memory", "2147480000");
	        
		JavaSparkContext sc = new JavaSparkContext("spark://10.3.36.255:7077", "First Spark App",conf);
		// 连接hdfs地址，读取输入内容，并分割处理
		JavaRDD<String> textFile = sc.textFile("hdfs://master:9000/flume/events/2018-09/*").flatMap(s -> Arrays.asList(s.split("\\s")).iterator());
		System.out.println(textFile.count());
//		JavaPairRDD<String, Integer> result = textFile.filter((String s) -> {
//			Boolean flag = false;
//			if (s.indexOf("<name>") != -1) {
//				flag = true;
//			} else if (s.indexOf("<property>") != -1) {
//				flag = true;
//			} else if (s.indexOf("<IF>") != -1) {
//				flag = true;
		//https://github.com/jinhang/ScalaApacheAccessLogParser/tree/master/src/main/scala
//			}
//			return flag;
//		}).mapToPair(word -> new Tuple2<>(word, 1)).reduceByKey((a, b) -> a + b);
//		result.saveAsTextFile("hdfs://localhost:9070/user/root/output2");
		//
	}

}