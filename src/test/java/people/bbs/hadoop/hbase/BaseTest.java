package people.bbs.hadoop.hbase;

import java.util.List;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.hadoop.hbase.HbaseTemplate;
import org.springframework.data.hadoop.hbase.RowMapper;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

@RunWith(SpringJUnit4ClassRunner.class) 
@ContextConfiguration(locations = {"classpath*:applicationContext.xml"}) 
public class BaseTest { 
 
    @Autowired 
    private HbaseTemplate template; 
 
    @Test 
    public void testFind() { 
        List<String> rows = template.find("user", "cf", "name", new RowMapper<String>() { 
            public String mapRow(Result result, int i) throws Exception { 
                return result.toString(); 
            } 
        }); 
        Assert.assertNotNull(rows); 
    } 
 
    @Test 
    public void testPut() { 
        template.put("user", "1", "cf", "name", Bytes.toBytes("Alice")); 
    } 
} 