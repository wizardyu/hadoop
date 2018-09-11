package people.bbs.hadoop.spark;
  
import java.text.SimpleDateFormat;  
import java.util.Date;  
  
import org.apache.spark.deploy.SparkSubmit;  
/** 
 * @author fansy 
 * 
 */  
public class SubmitScalaJobToSpark {  
  
    public static void main(String[] args) {  
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd-hh-mm-ss");   
        String filename = dateFormat.format(new Date());  
        String tmp=Thread.currentThread().getContextClassLoader().getResource("").getPath();  
        tmp =tmp.substring(0, tmp.length()-8);  
        String[] arg0=new String[]{  
                "--master","spark://node101:7077",  
                "--deploy-mode","client",  
                "--name","test java submit job to spark",  
                "--class","Scala_Test",  
                "--executor-memory","1G",  
//              "spark_filter.jar",  
                tmp+"lib/spark_filter.jar",//  
                "hdfs://node101:8020/user/root/log.txt",  
                "hdfs://node101:8020/user/root/badLines_spark_"+filename  
        };  
        SparkSubmit.main(arg0);  
    }  
}  