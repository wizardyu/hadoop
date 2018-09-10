package people.bbs.hadoop.hbase.util;

import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.hbase.client.Row;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.SQLContext;

import scala.Tuple2;
import scala.util.parsing.json.JSONObject;

public class Snippet {
	
	/**
     * 获取指定日期范围内的用户行为数据RDD
     * @param sqlContext
     * @param taskParam
     * @return
     */
    public static JavaRDD<Row> getActionRDDByDateRange(
            SQLContext sqlContext, JSONObject taskParam) {
        String startDate = ParamUtils.getParam(taskParam, Constants.PARAM_START_DATE);
        String endDate = ParamUtils.getParam(taskParam, Constants.PARAM_END_DATE);
 
        String sql = 
                "select * "
                + "from user_visit_action "
                + "where date>='" + startDate + "' "
                + "and date<='" + endDate + "'";  
 
        DataFrame actionDF = sqlContext.sql(sql);
 
        return actionDF.javaRDD();
    }
	

/**
     * 获取sessionid2到访问行为数据的映射的RDD
     * @param actionRDD 
     * @return
*/
public static JavaPairRDD<String, Row> getSessionid2ActionRDD(JavaRDD<Row> actionRDD) {
 
        return actionRDD.mapPartitionsToPair(new PairFlatMapFunction<Iterator<Row>, String, Row>() {
 
            private static final long serialVersionUID = 1L;
 
            @Override
            public Iterable<Tuple2<String, Row>> call(Iterator<Row> iterator)
                    throws Exception {
                List<Tuple2<String, Row>> list = new ArrayList<Tuple2<String, Row>>();
 
                while(iterator.hasNext()) {
                    Row row = iterator.next();
                    list.add(new Tuple2<String, Row>(row.getString(2), row));  
                }
 
                return list;
            }
 
        });
}
	/**
	     * 对行为数据按session粒度进行聚合
	     * @param actionRDD 行为数据RDD
	     * @return session粒度聚合数据
	    */
	    private static JavaPairRDD<String, String> aggregateBySession(
	            JavaSparkContext sc,
	            SQLContext sqlContext, 
	            JavaPairRDD<String, Row> sessinoid2actionRDD) {
	        // 对行为数据按session粒度进行分组
	        JavaPairRDD<String, Iterable<Row>> sessionid2ActionsRDD = 
	                sessinoid2actionRDD.groupByKey();
	 
	        // 对每一个session分组进行聚合，将session中所有的搜索词和点击品类都聚合起来
	        // 到此为止，获取的数据格式，如下：<userid,partAggrInfo(sessionid,searchKeywords,clickCategoryIds)>
	        JavaPairRDD<Long, String> userid2PartAggrInfoRDD = sessionid2ActionsRDD.mapToPair(
	 
	                new PairFunction<Tuple2<String,Iterable<Row>>, Long, String>() {
	 
	                    private static final long serialVersionUID = 1L;
	 
	                    @Override
	                    public Tuple2<Long, String> call(Tuple2<String, Iterable<Row>> tuple)
	                            throws Exception {
	                        String sessionid = tuple._1;
	                        Iterator<Row> iterator = tuple._2.iterator();
	 
	                        StringBuffer searchKeywordsBuffer = new StringBuffer("");
	                        StringBuffer clickCategoryIdsBuffer = new StringBuffer("");
	 
	                        Long userid = null;
	 
	                        // session的起始和结束时间
	                        Date startTime = null;
	                        Date endTime = null;
	                        // session的访问步长
	                        int stepLength = 0;
	 
	                        // 遍历session所有的访问行为
	                        while(iterator.hasNext()) {
	                            // 提取每个访问行为的搜索词字段和点击品类字段
	                            Row row = iterator.next();
	                            if(userid == null) {
	                                userid = row.getLong(1);
	                            }
	                            String searchKeyword = row.getString(5);
	                            Long clickCategoryId = row.getLong(6);
	 
	                            // 实际上这里要对数据说明一下
	                            // 并不是每一行访问行为都有searchKeyword何clickCategoryId两个字段的
	                            // 其实，只有搜索行为，是有searchKeyword字段的
	                            // 只有点击品类的行为，是有clickCategoryId字段的
	                            // 所以，任何一行行为数据，都不可能两个字段都有，所以数据是可能出现null值的
	 
	                            // 我们决定是否将搜索词或点击品类id拼接到字符串中去
	                            // 首先要满足：不能是null值
	                            // 其次，之前的字符串中还没有搜索词或者点击品类id
	 
	                            if(StringUtils.isNotEmpty(searchKeyword)) {
	                                if(!searchKeywordsBuffer.toString().contains(searchKeyword)) {
	                                    searchKeywordsBuffer.append(searchKeyword + ",");  
	                                }
	                            }
	                            if(clickCategoryId != null) {
	                                if(!clickCategoryIdsBuffer.toString().contains(
	                                        String.valueOf(clickCategoryId))) {   
	                                    clickCategoryIdsBuffer.append(clickCategoryId + ",");  
	                                }
	                            }
	 
	                            // 计算session开始和结束时间
	                            Date actionTime = DateUtils.parseTime(row.getString(4));
	 
	                            if(startTime == null) {
	                                startTime = actionTime;
	                            }
	                            if(endTime == null) {
	                                endTime = actionTime;
	                            }
	 
	                            if(actionTime.before(startTime)) {
	                                startTime = actionTime;
	                            }
	                            if(actionTime.after(endTime)) {
	                                endTime = actionTime;
	                            }
	 
	                            // 计算session访问步长
	                            stepLength++;
	                        }
	 
	                        String searchKeywords = StringUtils.trimComma(searchKeywordsBuffer.toString());
	                        String clickCategoryIds = StringUtils.trimComma(clickCategoryIdsBuffer.toString());
	 
	                        // 计算session访问时长（秒）
	                        long visitLength = (endTime.getTime() - startTime.getTime()) / 1000; 
	 
	                        // 大家思考一下
	                        // 我们返回的数据格式，即使<sessionid,partAggrInfo>
	                        // 但是，这一步聚合完了以后，其实，我们是还需要将每一行数据，跟对应的用户信息进行聚合
	                        // 问题就来了，如果是跟用户信息进行聚合的话，那么key，就不应该是sessionid
	                        // 就应该是userid，才能够跟<userid,Row>格式的用户信息进行聚合
	                        // 如果我们这里直接返回<sessionid,partAggrInfo>，还得再做一次mapToPair算子
	                        // 将RDD映射成<userid,partAggrInfo>的格式，那么就多此一举
	 
	                        // 所以，我们这里其实可以直接，返回的数据格式，就是<userid,partAggrInfo>
	                        // 然后跟用户信息join的时候，将partAggrInfo关联上userInfo
	                        // 然后再直接将返回的Tuple的key设置成sessionid
	                        // 最后的数据格式，还是<sessionid,fullAggrInfo>
	 
	                        // 聚合数据，用什么样的格式进行拼接？
	                        // 我们这里统一定义，使用key=value|key=value
	                        String partAggrInfo = Constants.FIELD_SESSION_ID + "=" + sessionid + "|"
	                                + Constants.FIELD_SEARCH_KEYWORDS + "=" + searchKeywords + "|"
	                                + Constants.FIELD_CLICK_CATEGORY_IDS + "=" + clickCategoryIds + "|"
	                                + Constants.FIELD_VISIT_LENGTH + "=" + visitLength + "|"
	                                + Constants.FIELD_STEP_LENGTH + "=" + stepLength + "|"
	                                + Constants.FIELD_START_TIME + "=" + DateUtils.formatTime(startTime);    
	 
	                        return new Tuple2<Long, String>(userid, partAggrInfo);
	                    }
	 
	                });
	 
	        // 查询所有用户数据，并映射成<userid,Row>的格式
	        String sql = "select * from user_info";  
	        JavaRDD<Row> userInfoRDD = sqlContext.sql(sql).javaRDD();
	 
	        JavaPairRDD<Long, Row> userid2InfoRDD = userInfoRDD.mapToPair(
	 
	                new PairFunction<Row, Long, Row>() {
	 
	                    private static final long serialVersionUID = 1L;
	 
	                    @Override
	                    public Tuple2<Long, Row> call(Row row) throws Exception {
	                        return new Tuple2<Long, Row>(row.getLong(0), row);
	                    }
	 
	                });
	 
	        /**
	         * 这里就可以说一下，比较适合采用reduce join转换为map join的方式
	         * 
	         * userid2PartAggrInfoRDD，可能数据量还是比较大，比如，可能在1千万数据
	         * userid2InfoRDD，可能数据量还是比较小的，你的用户数量才10万用户
	         * 
	         */
	 
	        // 将session粒度聚合数据，与用户信息进行join
	        JavaPairRDD<Long, Tuple2<String, Row>> userid2FullInfoRDD = 
	                userid2PartAggrInfoRDD.join(userid2InfoRDD);
	 
	        // 对join起来的数据进行拼接，并且返回<sessionid,fullAggrInfo>格式的数据
	        JavaPairRDD<String, String> sessionid2FullAggrInfoRDD = userid2FullInfoRDD.mapToPair(
	 
	                new PairFunction<Tuple2<Long,Tuple2<String,Row>>, String, String>() {
	 
	                    private static final long serialVersionUID = 1L;
	 
	                    @Override
	                    public Tuple2<String, String> call(
	                            Tuple2<Long, Tuple2<String, Row>> tuple)
	                            throws Exception {
	                        String partAggrInfo = tuple._2._1;
	                        Row userInfoRow = tuple._2._2;
	 
	                        String sessionid = StringUtils.getFieldFromConcatString(
	                                partAggrInfo, "\\|", Constants.FIELD_SESSION_ID);
	 
	                        int age = userInfoRow.getInt(3);
	                        String professional = userInfoRow.getString(4);
	                        String city = userInfoRow.getString(5);
	                        String sex = userInfoRow.getString(6);
	 
	                        String fullAggrInfo = partAggrInfo + "|"
	                                + Constants.FIELD_AGE + "=" + age + "|"
	                                + Constants.FIELD_PROFESSIONAL + "=" + professional + "|"
	                                + Constants.FIELD_CITY + "=" + city + "|"
	                                + Constants.FIELD_SEX + "=" + sex;
	 
	                        return new Tuple2<String, String>(sessionid, fullAggrInfo);
	                    }
	 
	                });
	 
	 
	        return sessionid2FullAggrInfoRDD;
	
}
}

