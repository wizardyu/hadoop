package people.bbs.hadoop.spark;
import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;

import scala.Tuple2;

/** 
 * Hello world! 
 * 
 */  
public class App {  
    public static void main(String[] args) {  
        SparkConf conf = new SparkConf().setMaster("local").setAppName("Simple Application");  
        JavaSparkContext sc = new JavaSparkContext(conf);  

        // convert from other RDD  
        JavaRDD<String> line1 = sc.parallelize(Arrays.asList("1 aa", "2 bb", "4 cc", "3 dd")); 

        line1.foreach(new VoidFunction<String>(){

            @Override
            public void call(String num) throws Exception {
                // TODO Auto-generated method stub
                System.out.println("numbers;"+num);
            }
        });

        JavaPairRDD<String, String> prdd = line1.mapToPair(new PairFunction<String, String, String>() {  
            public Tuple2<String, String> call(String x) throws Exception {  
                return new Tuple2(x.split(" ")[0], x.split(" ")[1]);  
            }  
        });  
        System.out.println("111111111111mapToPair:");  
        prdd.foreach(new VoidFunction<Tuple2<String, String>>() {  
            public void call(Tuple2<String, String> x) throws Exception {  
                System.out.println(x);  
            }  
        });  

       /* JavaRDD => JavaPairRDD: 通过mapToPair函数
        JavaPairRDD => JavaRDD: 通过map函数转换*/

        System.out.println("===============1=========");  
        JavaRDD<String> javaprdd =prdd.map(new Function<Tuple2<String,String>,String>() {
            private static final long serialVersionUID = 1L;
            @Override
            public String call(Tuple2<String, String> arg0)  {
            // TODO Auto-generated method stub
                System.out.println("arg0======================"+arg0);
                System.out.println("arg0======================"+arg0._1);
                 return arg0._1+" "+arg0._2;
            }

       });

        System.out.println("===============2=========");  

        javaprdd.foreach(new VoidFunction<String>(){

            @Override
            public void call(String num) throws Exception {
                // TODO Auto-generated method stub
                System.out.println("numbers;"+num);
            }
        });

    }  
}  