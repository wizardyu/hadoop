package people.bbs.hadoop.spark.hbase;

import java.io.IOException;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

import com.bigdata.spark.SparkManager;
import com.bigdata.spark.pojo.Customer;
import com.bigdata.spark.sql.DataFrameManger;

import scala.Tuple2;

 

public class SparkHbaseTest {

private  static final Pattern SPACE = Pattern.compile("\\|");

/**

* @param args

*/

public static void main(String[] args) {

SparkConf sparkConf = new SparkConf();

sparkConf.setAppName("sparkHbase");

sparkConf.setMaster("local");

try {

String tableName="customer";

//获取 JavaSparkContext

JavaSparkContext jsc = SparkManager.getInstance().getJavaSparkContxt(sparkConf);

//使用HBaseConfiguration.create()生成Configuration

Configuration conf = HBaseConfiguration.create();

conf.set("hbase.zookeeper.property.clientPort", "4180");  

conf.set("hbase.zookeeper.quorum", "192.168.68.84");  

//conf.set("hbase.master", "192.168.68.84:60000");

conf.set(TableInputFormat.INPUT_TABLE, tableName);

 

createTable(conf,tableName);

 

final Broadcast<String> broadcastTableName = jsc.broadcast(tableName);

 

SQLContext sqlContext = SparkManager.getInstance().getSQLContext(jsc);

   

   System.out.println("=== Data source: RDD ===");

   //将文件内容信息转换为 java bean

   JavaRDD<String> jrdd = SparkManager.getInstance().getJavaRDD(jsc,"hdfs://192.168.68.84:9000/storm/app_hdfsBolt-5-1-1434442141499.log");

   JavaRDD<Customer> customerRdd = jrdd.map(

     new Function<String, Customer>() {

       @Override

       public Customer call(String line) {

         String[] parts = line.split("\\|");

         Customer customer = new Customer();

         customer.setId(parts[0]);

         customer.setCustomcode(parts[1]);

         customer.setCode(parts[2]);

         customer.setPrice(Double.parseDouble(parts[3]));

         return customer;

       }

     });

   //将 JavaRDD<Customer> 对象 序列化一个  tcustomer 表

   DataFrame schemaCustomer = DataFrameManger.getInstance().getDataFrameByFile(sqlContext, customerRdd, Customer.class);

   schemaCustomer.registerTempTable("tcustomer");

   

// 从表 tcustomer获取  符合条件的数据

   DataFrame teenagers = sqlContext.sql("SELECT id,customcode,code,price FROM tcustomer  WHERE price >= 110 AND price <= 500");

 //从结果做获取数据，  

  List<Object> clist = teenagers.toJavaRDD().map(new Function<Row, Object>() {

 

@Override

public Object call(Row row) throws Exception {

Customer cust = new Customer();

cust.setId(row.getString(0));

cust.setCustomcode(row.getString(1));

cust.setCode(row.getString(2));

cust.setPrice(row.getDouble(3));

return cust;

}

   

}).collect();

  

  //将数据转换为 javapairRDD 对象

  JavaPairRDD<String, Customer> jpairCust = teenagers.toJavaRDD().mapToPair(new PairFunction<Row, String, Customer>() {

@Override

public Tuple2<String, Customer> call(Row row) throws Exception {

Customer cust = new Customer();

cust.setId(row.getString(0));

cust.setCustomcode(row.getString(1));

cust.setCode(row.getString(2));

cust.setPrice(row.getDouble(3));

return new Tuple2<String, Customer>(row.getString(0), cust);

}

});

  

  jpairCust.foreach(new VoidFunction<Tuple2<String,Customer>>() {

 

@Override

public void call(Tuple2<String, Customer> t) throws Exception {

insertData(broadcastTableName.value(), t._2());

System.out.println("key:"+t._1()+"==code:"+t._2().getCustomcode());

}

});

  

   for (Object obj: clist) {

   Customer ct = (Customer) obj;

     System.out.println(ct.getId()+" "+ct.getCustomcode()+" "+ct.getCode()+" "+ct.getPrice());

   }

   jsc.stop();

} catch (Exception e) {

// TODO Auto-generated catch block

e.printStackTrace();

}

 

}

 

/**

* 创建表

* @param conf

* @param tableName

*/

private static void createTable(Configuration conf,String tableName){

System.out.println("start create table ......");  

       try {  

           HBaseAdmin hBaseAdmin = new HBaseAdmin(conf);  

           if (hBaseAdmin.tableExists(tableName)) {// 如果存在要创建的表，那么先删除，再创建  

               hBaseAdmin.disableTable(tableName);  

               hBaseAdmin.deleteTable(tableName);  

               System.out.println(tableName + " is exist,detele....");  

           }  

           HTableDescriptor tableDescriptor = new HTableDescriptor(tableName);  

           tableDescriptor.addFamily(new HColumnDescriptor("column1"));  

           tableDescriptor.addFamily(new HColumnDescriptor("column2"));  

           tableDescriptor.addFamily(new HColumnDescriptor("column3"));

           tableDescriptor.addFamily(new HColumnDescriptor("column4"));

           hBaseAdmin.createTable(tableDescriptor);  

       } catch (MasterNotRunningException e) {  

           e.printStackTrace();  

       } catch (ZooKeeperConnectionException e) {  

           e.printStackTrace();  

       } catch (IOException e) {  

           e.printStackTrace();  

       }  

       System.out.println("end create table ......");  

}

 

/** 

     * 插入数据 

     * @param tableName 

* @throws IOException 

     */  

    public static void insertData(String tableName,Customer cust) throws IOException {  

        System.out.println("start insert data ......");

        Configuration conf = HBaseConfiguration.create();

conf.set("hbase.zookeeper.property.clientPort", "4180");  

conf.set("hbase.zookeeper.quorum", "192.168.68.84");  

conf.set(TableInputFormat.INPUT_TABLE, tableName);

        HTable table = new HTable(conf,tableName);  

        Put put = new Put("Customer".getBytes());// 一个PUT代表一行数据，再NEW一个PUT表示第二行数据,每行一个唯一的ROWKEY，此处rowkey为put构造方法中传入的值  

        put.add("column1".getBytes(), null, cust.getId().getBytes());// 本行数据的第一列  

        put.add("column2".getBytes(), null, cust.getCustomcode().getBytes());// 本行数据的第二列  

        put.add("column3".getBytes(), null, cust.getCode().getBytes());// 本行数据的第三列

        put.add("column4".getBytes(), null, Double.toString(cust.getPrice()).getBytes());// 本行数据的第四列

        try {  

            table.put(put);  

        } catch (IOException e) {  

            e.printStackTrace();  

        }  

        System.out.println("end insert data ......");  

    } 

 

}