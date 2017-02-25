package com.graphdatabases.neo4j;

import org.neo4j.driver.v1.*;

import org.apache.log4j.Logger;

import java.io.FileInputStream;
import java.util.Properties;

public class PerformanceAnalysis {

    final static Logger logger = Logger.getLogger(PerformanceAnalysis.class);

    private Driver driver;

    public PerformanceAnalysis(Driver driver)
    {
        this.driver = driver;
    }

    public void closeConnection()
    {
        driver.close();
    }

    public void findMostConnectedNode()
    {
        Session session = driver.session();
        StatementResult result = session.run(
                "START n = node(*) MATCH p = (n)-[:FRIENDS]-() RETURN n.id as nodeID, count(p) as count ORDER BY count desc LIMIT 1");
        while (result.hasNext()){
            Record record = result.next();
            System.out.println("Node ID: " + record.get("nodeID").asString());
            System.out.println("Number of connections: " +  record.get("count").asInt());
        }

        session.close();
    }

    public static void main(String[] args)
    {
        Properties properties = new Properties();
        try {
            String path = PerformanceAnalysis.class.getResource("driver-settings.properties").getPath();
            FileInputStream in = new FileInputStream(path);
            properties.load(in);
            in.close();
        } catch (Exception e) {
            logger.error(e);
        }

        String uri = properties.getProperty("uri");
        String username = properties.getProperty("username");
        String password = properties.getProperty("password");
        Driver driver = GraphDatabase.driver(uri, AuthTokens.basic( username, password ));
        PerformanceAnalysis analysis = new PerformanceAnalysis(driver);

        logger.info("Start performing tests.");
        analysis.findMostConnectedNode();

        logger.info("Closing connection.");
        analysis.closeConnection();
    }
}
