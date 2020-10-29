package com.atguigu.hdfs;



import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class HdfsClient {

    private FileSystem fs;
    @Before  //before 方法在test方法之前运行一次
    public void init() throws URISyntaxException, IOException, InterruptedException {
        fs = FileSystem.get(new URI("hdfs://hadoop102:9820"), new Configuration(),"atguigu");
    }
    @After   //after 方法在test方法之后运行一次
    public void close() throws IOException {
        fs.close();
    }

    @Test
    public void testMkdirs() throws IOException, URISyntaxException, InterruptedException {

        fs.mkdirs(new Path("/jave3"));
    }

    @Test
    public void testCopyFromLocalFile() throws IOException {
        fs.copyFromLocalFile(new Path("D://xxxxxx//xxxxxx123.txt"),new Path("/java2/www.txt"));
    }
    @Test
    public void test1() throws IOException {
        fs.delete(new Path("/java2"),true);
    }
    @Test
    public void testRename() throws IOException {
        fs.rename(new Path("/sanguo/kongming.txt"),new Path("/sanguo/zhuge.txt"));
    }

    //hdfs文件详情查看
    @Test
    public void testListFiles() throws IOException {
        //1.获取文件详情
        RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path("/"), true);
        while(listFiles.hasNext()){
            LocatedFileStatus next = listFiles.next();
            //输出详情
            //文件名称
            System.out.println(next.getPath().getName());
            //长度
            System.out.println(next.getLen());
            //权限
            System.out.println(next.getPermission());

            BlockLocation[] blockLocations = next.getBlockLocations();
            for (BlockLocation blockLocation : blockLocations) {
                String[] hosts = blockLocation.getHosts();
                for (String host : hosts) {
                    System.out.println(host);
                }
            }
        }
    }
    @Test
    public void testListStatus() throws IOException {
        //1.获取文件配置信息
        FileStatus[] fileStatuses = fs.listStatus(new Path("/sanguo/zhuge.txt"));
        for (FileStatus fileStatus : fileStatuses) {
            if(fileStatus.isFile()){
                System.out.println("f:" + fileStatus.getPath().getName());
            }else{
                System.out.println("d:" + fileStatus.getPath().getName());
            }
        }
    }
}
