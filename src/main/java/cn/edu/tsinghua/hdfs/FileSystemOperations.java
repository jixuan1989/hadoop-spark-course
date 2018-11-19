package cn.edu.tsinghua.hdfs;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * modified from https://gist.githubusercontent.com/ashrithr/f7899fdfd36ee800f151/raw/6b4678a2e406b54ff3ddc56d149664b876b52e33/FIleSystemOperations.java
 *
 */
public class FileSystemOperations {
    public FileSystemOperations() {

    }

    /**
     * create a existing file from local filesystem to hdfs
     * @param source
     * @param dest
     * @param conf
     * @throws IOException
     */
    public void addFile(String source, String dest, Configuration conf) throws IOException {

        FileSystem fileSystem = FileSystem.get(conf);

        // Get the filename out of the file path
        String filename = source.substring(source.lastIndexOf('/') + 1,source.length());

        // Create the destination path including the filename.
        if (dest.charAt(dest.length() - 1) != '/') {
            dest = dest + "/" + filename;
        } else {
            dest = dest + filename;
        }

        // System.out.println("Adding file to " + destination);

        // Check if the file already exists
        Path path = new Path(dest);
        if (fileSystem.exists(path)) {
            System.out.println("File " + dest + " already exists");
            return;
        }

        // Create a new file and write data to it.
        FSDataOutputStream out = fileSystem.create(path);
        InputStream in = new BufferedInputStream(new FileInputStream(new File(
                source)));

        byte[] b = new byte[1024];
        int numBytes = 0;
        while ((numBytes = in.read(b)) > 0) {
            out.write(b, 0, numBytes);
        }

        // Close all the file descriptors
        in.close();
        out.close();
        fileSystem.close();
    }

    /**
     * read a file from hdfs
     * @param file
     * @param conf
     * @throws IOException
     */
    public void readFile(String file, Configuration conf) throws IOException {
        FileSystem fileSystem = FileSystem.get(conf);

        Path path = new Path(file);
        if (!fileSystem.exists(path)) {
            System.out.println("File " + file + " does not exists");
            return;
        }

        FSDataInputStream in = fileSystem.open(path);

        String filename = file.substring(file.lastIndexOf('/') + 1,
                file.length());

        OutputStream out = new BufferedOutputStream(new FileOutputStream(
                new File(filename)));

        byte[] b = new byte[1024];
        int numBytes = 0;
        while ((numBytes = in.read(b)) > 0) {
            out.write(b, 0, numBytes);
        }

        in.close();
        out.close();
        fileSystem.close();
    }

    /**
     * delete a directory in hdfs
     * @param file
     * @throws IOException
     */
    public void deleteFile(String file, Configuration conf) throws IOException {
        FileSystem fileSystem = FileSystem.get(conf);

        Path path = new Path(file);
        if (!fileSystem.exists(path)) {
            System.out.println("File " + file + " does not exists");
            return;
        }

        fileSystem.delete(new Path(file), true);

        fileSystem.close();
    }

    /**
     * create directory in hdfs
     * @param dir
     * @throws IOException
     */
    public void mkdir(String dir, Configuration conf) throws IOException {
        FileSystem fileSystem = FileSystem.get(conf);

        Path path = new Path(dir);
        if (fileSystem.exists(path)) {
            System.out.println("Dir " + dir + " already not exists");
            return;
        }

        fileSystem.mkdirs(path);

        fileSystem.close();
    }

    public static void main(String[] args) throws IOException {

//        args = new String[]{"192.144.187.79", "9000", "mkdir", "/test"};
//        args = new String[]{"192.144.187.79", "9000", "add", ".gitignore", "/test"};
        args = new String[]{"192.144.187.79", "9000", "delete", "/test/.gitignore"};

        if (args.length < 1) {
            System.out.println("Usage: hdfsmaster add/read/delete/mkdir"
                    + " [<local_path> <hdfs_path>]");
            System.exit(1);
        }

        FileSystemOperations client = new FileSystemOperations();

        Configuration conf = new Configuration();
        System.getProperties().put("HADOOP_USER_NAME","hxd");
        String hdfsPath = "hdfs://" + args[0] + ":" + args[1];
        conf.set("fs.default.name", hdfsPath);

        if (args[2].equals("add")) {
            if (args.length < 5) {
                System.out.println("Usage: hdfsclient add <local_path> "
                        + "<hdfs_path>");
                System.exit(1);
            }

            client.addFile(args[3], args[4], conf);

        } else if (args[2].equals("read")) {
            if (args.length < 4) {
                System.out.println("Usage: hdfsclient read <hdfs_path>");
                System.exit(1);
            }

            client.readFile(args[3], conf);

        } else if (args[2].equals("delete")) {
            if (args.length < 4) {
                System.out.println("Usage: hdfsclient delete <hdfs_path>");
                System.exit(1);
            }

            client.deleteFile(args[3], conf);

        } else if (args[2].equals("mkdir")) {
            if (args.length < 4) {
                System.out.println("Usage: hdfsclient mkdir <hdfs_path>");
                System.exit(1);
            }

            client.mkdir(args[3], conf);

        } else {
            System.out.println("Usage: hdfsclient add/read/delete/mkdir"
                    + " [<local_path> <hdfs_path>]");
            System.exit(1);
        }

        System.out.println("Done!");
    }
}
