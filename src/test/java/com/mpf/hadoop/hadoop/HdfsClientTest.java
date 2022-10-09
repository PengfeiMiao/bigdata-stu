package com.mpf.hadoop.hadoop;

import org.junit.jupiter.api.*;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class HdfsClientTest {

    static HdfsClient client;

    @BeforeAll
    static void setUp() {
        client = new HdfsClient();
        client.delete("/dir1");
        client.delete("/dir2");
        client.delete("/dir11");
    }

    @AfterAll
    static void tearDown() {
        client.closeConn();
    }

    @Order(1)
    @Test
    public void testMkdir() {
        Assertions.assertTrue(client.mkdir("/dir1"));
        Assertions.assertTrue(client.mkdir("/dir2"));
    }

    @Order(2)
    @Test
    public void testPut() {
        Assertions.assertTrue(client.put("./wordcount/input/file1.txt", "/dir1"));
    }

    @Order(3)
    @Test
    public void testDetail() {
        // 查看文件信息
        String fileDetail = client.detail("/dir1/file1.txt");
        System.out.println("detail:\n" + fileDetail);
        Assertions.assertTrue(fileDetail.contains("file1.txt"));
        Assertions.assertTrue(fileDetail.contains("pengfei.miao"));
    }

    @Order(4)
    @Test
    public void testGet() {
        Assertions.assertTrue(client.get("/dir1/file1.txt", "./wordcount/output/"));
    }

    @Order(5)
    @Test
    public void testMove() {
        // 移动文件
        Assertions.assertTrue(client.move("/dir1/file1.txt", "/dir2"));
        // 重命名文件
        Assertions.assertTrue(client.move("/dir2/file1.txt", "/dir2/file11.txt"));
        // 重命名文件夹
        Assertions.assertTrue(client.move("/dir1", "/dir11"));
    }

    @Order(6)
    @Test
    public void testDelete() {
        // 删除文件
        Assertions.assertTrue(client.delete("/dir2/file11.txt"));
        // 删除文件夹
        Assertions.assertTrue(client.delete("/dir2"));
        Assertions.assertTrue(client.delete("/dir11"));
    }

}