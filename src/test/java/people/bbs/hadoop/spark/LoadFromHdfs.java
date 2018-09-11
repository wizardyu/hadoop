package people.bbs.hadoop.spark;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class LoadFromHdfs {

    public static void main(String[] argvs) {
        String uri = "hdfs://master:9000/flume/events/2018-09/";
        Configuration conf = new Configuration();
        //出现无法识别hdsf的问题可以加上以下两句
        conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
        conf.set("fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem");
        try {
            FileSystem fs = FileSystem.get(new URI(uri), conf);
            Path path = new Path(uri);
            FileStatus status[] = fs.listStatus(path);
            for (int i = 0; i < status.length; i++) {
                String pathname =status[i].getPath().toString();
                System.out.println(pathname);
                System.out.println("==========");
                FileStatus tmp = status[i];
                if(tmp.isFile()&&tmp.getLen()>0){//判断文件大小
                    InputStream  in = fs.open(tmp.getPath());
                    StringBuffer sb = new StringBuffer("");
                    byte[] b = new byte[1];
                    while (in.read(b) != -1) {
                        // 字符串拼接
                        sb.append(new String(b));
                    }
                    System.out.println(sb.toString());
                    System.out.println("==========");
                    in.close();
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }
    }

}