package cn.edu.tsinghua.hdfs;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class WriteFile {

    public static void main(String[] args) throws IOException {
        Configuration conf = new Configuration();
        conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
        String hdfsPath = "hdfs://node1:9000";
        conf.set("fs.default.name", hdfsPath);

        FileSystem fs = FileSystem.get(conf);

        Path path = new Path("/mydata.txt");
        FSDataOutputStream out = fs.create(path);
        out.writeUTF("da jia hao,cai shi zhen de hao!");

        fs.close();
    }
}