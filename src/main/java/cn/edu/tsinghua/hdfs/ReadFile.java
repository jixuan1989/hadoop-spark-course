package cn.edu.tsinghua.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URI;

public class ReadFile {

    public static void main(String[] args) {
        try {
            Configuration conf = new Configuration();
            FileSystem fs = FileSystem.get(new URI("hdfs://localhost:9000"), conf);
            Path file = new Path("/output/part-r-00000");
            FSDataInputStream getIt = fs.open(file);
            BufferedReader d = new BufferedReader(new InputStreamReader(getIt));
            String s;
            while ((s = d.readLine()) != null) {
                System.out.println(s);
            }
            d.close();
            fs.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
