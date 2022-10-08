package com.mpf.hadoop.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;


public class HdfsClient {

//    private static final Logger log = LoggerFactory.getLogger(HdfsClient.class);

    public FileSystem fs;

    public HdfsClient() {
        this.fs = getConn();
    }

    private FileSystem getConn() {
        Configuration conf = new Configuration();
        conf.addResource("classpath:hdfs-site.yml");
        FileSystem fs;
        try {
            fs = FileSystem.get(new URI("hdfs://localhost:9000"), conf);
        } catch (URISyntaxException | IOException e) {
            throw new RuntimeException(e);
        }
        return fs;
    }

    public boolean mkdir(String filePath) {
        boolean result;
        try {
            result = fs.mkdirs(new Path(filePath));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        return result;
    }

    public boolean put(String srcPath, String dstPath) {
        boolean result = true;
        try {
            fs.copyFromLocalFile(new Path(srcPath), new Path(dstPath));
        } catch (IOException e) {
            result = false;
            e.printStackTrace();
        }
        return result;
    }

    public boolean get(String srcPath, String dstPath) {
        boolean result = true;
        try {
            // useRawLocalFileSystem: 是否使用 crc校验和 检测文件完整性，默认使用，true 则不使用
            fs.copyToLocalFile(false, new Path(srcPath), new Path(dstPath), true);
        } catch (IOException e) {
            result = false;
            e.printStackTrace();
        }
        return result;
    }

    public boolean delete(String filePath) {
        boolean result = false;
        try {
            // recursive: 是否递归删除
            result = fs.delete(new Path(filePath), true);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return result;
    }

    public boolean move(String srcPath, String dstPath) {
        boolean result = false;
        try {
            result = fs.rename(new Path(srcPath), new Path(dstPath));
        } catch (IOException e) {
            e.printStackTrace();
        }
        return result;
    }

    public String detail(String filePath) {
        StringBuilder result = new StringBuilder();
        try {
            RemoteIterator<LocatedFileStatus> files = fs.listFiles(new Path(filePath), true);
            while(files.hasNext()) {
                LocatedFileStatus fileStatus = files.next();

                result.append("path: ").append(fileStatus.getPath());
                result.append("\nisFile: ").append(fileStatus.isFile());
                result.append("\nname: ").append(fileStatus.getPath().getName());
                result.append("\npermission: ").append(fileStatus.getPermission());
                result.append("\nowner: ").append(fileStatus.getOwner());
                result.append("\ngroup: ").append(fileStatus.getGroup());
                result.append("\nlen: ").append(fileStatus.getLen());
                result.append("\nmodificationTime: ").append(fileStatus.getModificationTime());
                result.append("\nreplication: ").append(fileStatus.getReplication());
                result.append("\nblockSize: ").append(fileStatus.getBlockSize());
                // 获取块信息
                BlockLocation[] blockLocations = fileStatus.getBlockLocations();
                result.append("\nblockLocations: ").append(Arrays.toString(blockLocations));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return result.toString();
    }

    public void closeConn() {
        try {
            this.fs.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
