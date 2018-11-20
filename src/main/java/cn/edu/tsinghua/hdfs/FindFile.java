package cn.edu.tsinghua.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class FindFile {

    public static void main(String[] args) throws IOException, URISyntaxException {
        getFileLocal("/mydata.txt");
    }

    public static void getFileLocal(String file) throws IOException, URISyntaxException {
        Configuration conf = new Configuration();
        conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
        FileSystem fs = FileSystem.get(new URI("hdfs://localhost:9000"), conf);
        Path path = new Path(file);

        FileStatus status = fs.getFileStatus(path);
        BlockLocation[] locations = fs.getFileBlockLocations(status, 0, status.getLen());

        int length = locations.length;
        for (int i = 0; i < length; i++) {
            String[] hosts = locations[i].getHosts();
            System.out.println("block_" + i + "_location:" + hosts[i]);
        }
    }
}
