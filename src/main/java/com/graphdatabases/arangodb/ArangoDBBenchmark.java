package com.graphdatabases.arangodb;

import com.arangodb.ArangoDB;
import com.graphdatabases.benchmark.BenchmarkTest;
import com.graphdatabases.benchmark.annotation.Setup;
import com.graphdatabases.benchmark.annotation.TearDown;

import java.io.FileInputStream;
import java.util.Properties;

public class ArangoDBBenchmark {

    private ArangoDB arangoDB;

    @Setup
    public void setup() {

        System.out.println("Initializing ArangoDB database driver.");
        initializeDriver();
    }

    private void initializeDriver() {
        Properties properties = new Properties();
        try {
            String path = ArangoDBBenchmark.class.getResource("driver-settings.properties").getPath();
            FileInputStream in = new FileInputStream(path);
            properties.load(in);
            in.close();
        } catch (Exception e) {
            System.out.println("Failed to load driver settings.");
        }

        String host = properties.getProperty("host");
        Integer port = Integer.parseInt(properties.getProperty("port"));
        String username = properties.getProperty("username");
        String password = properties.getProperty("password");

        arangoDB = new ArangoDB.Builder().host(host, port).user(username).password(password).build();
    }

    @TearDown
    public void tearDown() {
        arangoDB.shutdown();
    }

    public static void main(String[] args) {
        BenchmarkTest benchmarkTest = new BenchmarkTest(ArangoDBBenchmark.class);
        benchmarkTest.run();
    }
}
