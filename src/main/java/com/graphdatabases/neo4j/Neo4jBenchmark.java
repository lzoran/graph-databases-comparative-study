package com.graphdatabases.neo4j;

import com.graphdatabases.benchmark.BenchmarkTest;
import com.graphdatabases.benchmark.annotation.Benchmark;
import com.graphdatabases.benchmark.annotation.Setup;
import com.graphdatabases.benchmark.annotation.TearDown;
import org.neo4j.driver.v1.*;

import java.io.*;
import java.util.Properties;

public class Neo4jBenchmark {

    private Driver driver;

    @Setup
    public void setup() {

        System.out.println("Initializing database driver.");
        initializeDriver();

        System.out.println("Cleaning database.");
        clean();

        System.out.println("Creating index on property nodeId.");
        createIndexOnProperty("nodeId");

        System.out.println("Setting initial data.");
        setupInitialData();
    }

    private void initializeDriver() {
        Properties properties = new Properties();
        try {
            String path = Neo4jBenchmark.class.getResource("driver-settings.properties").getPath();
            FileInputStream in = new FileInputStream(path);
            properties.load(in);
            in.close();
        } catch (Exception e) {
            System.out.println("Failed to load driver settings.");
        }

        String uri = properties.getProperty("uri");
        String username = properties.getProperty("username");
        String password = properties.getProperty("password");

        driver = GraphDatabase.driver(uri, AuthTokens.basic(username, password));
    }

    private void clean() {
        Session session = driver.session();
        session.run("MATCH (n) DETACH DELETE n");
        session.close();
    }

    private void createIndexOnProperty(String propertyName) {
        Session session = driver.session();
        String statement = String.format("CREATE INDEX ON :Person(%s)", propertyName);
        session.run(statement);
        session.close();
    }

    private void setupInitialData() {
        String path = Neo4jBenchmark.class.getResource("/datasets/facebook/facebook_combined.txt/facebook_combined.txt").getPath();
        try {
            BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(path)));
            String line;
            while ((line = br.readLine()) != null) {
                String[] nodes = line.trim().split(" ");
                if (nodes.length != 2) {
                    System.out.println(String.format("Invalid data detected: [%s]", line));
                    continue;
                }

                Integer nodeOneId = Integer.parseInt(nodes[0]);
                Integer nodeTwoId = Integer.parseInt(nodes[1]);

                if (!checkIfNodeExists(nodeOneId)) {
                    createNode(nodeOneId);
                }

                if (!checkIfNodeExists(nodeTwoId)) {
                    createNode(nodeTwoId);
                }

                createRelationship(nodeOneId, nodeTwoId);
            }
        } catch (FileNotFoundException e) {
            System.out.println("Failed to load data.");
        } catch (IOException e) {
            System.out.println(String.format("Exception: %s", e.getMessage()));
        }
    }

    public boolean checkIfNodeExists(Integer nodeId) {
        boolean exists = false;

        Session session = driver.session();
        String statement = String.format("MATCH (p:Person { nodeId:%d }) RETURN p", nodeId);
        StatementResult result = session.run(statement);
        if (result.hasNext()) {
            exists = true;
        }
        session.close();

        return exists;
    }

    public StatementResult createNode(Integer nodeId) {
        Session session = driver.session();
        String statement = String.format("CREATE (p:Person { nodeId: %d}) RETURN p", nodeId);
        StatementResult result = session.run(statement);
        session.close();

        return result;
    }

    public StatementResult createRelationship(Integer firstNode, Integer secondNode) {
        Session session = driver.session();
        String statement = String.format("MATCH (p1:Person), (p2:Person) WHERE p1.nodeId = %d AND p2.nodeId = %d CREATE (p1)-[r1:FRIEND]->(p2) CREATE (p2)-[r2:FRIEND]->(p1) RETURN r1, r2", firstNode, secondNode);
        StatementResult result = session.run(statement);
        session.close();

        return result;
    }

    @Benchmark(iteration = 10)
    public StatementResult findNodeWithLeastIngoingEdges() {
        Session session = driver.session();
        String statement = "MATCH (:Person)-[r:FRIEND]->(p:Person) RETURN p.nodeId, count(r) as count ORDER BY count ASC LIMIT 1";
        StatementResult result = session.run(statement);
        session.close();

        return result;
    }

    @Benchmark(iteration = 10)
    public StatementResult findNodeWithLeastOutgoingEdges() {
        Session session = driver.session();
        String statement = "MATCH (p:Person)-[r:FRIEND]->(:Person) RETURN p.nodeId, count(r) as count ORDER BY count ASC LIMIT 1";
        StatementResult result = session.run(statement);
        session.close();

        return result;
    }

    @Benchmark(iteration = 10)
    public StatementResult findNodeWithLeastIngoingAndOutgoingEdges() {
        Session session = driver.session();
        String statement = "MATCH (p:Person)-[r:FRIEND]-(:Person) RETURN p.nodeId, count(r) as count ORDER BY count ASC LIMIT 1";
        StatementResult result = session.run(statement);
        session.close();

        return result;
    }

    @Benchmark(iteration = 10)
    public StatementResult findNodeWithMostIngoingEdges() {
        Session session = driver.session();
        String statement = "MATCH (:Person)-[r:FRIEND]->(p:Person) RETURN p.nodeId, count(r) as count ORDER BY count DESC LIMIT 1";
        StatementResult result = session.run(statement);
        session.close();

        return result;
    }

    @Benchmark(iteration = 10)
    public StatementResult findNodeWithMostOutgoingEdges() {
        Session session = driver.session();
        String statement = "MATCH (p:Person)-[r:FRIEND]->(:Person) RETURN p.nodeId, count(r) as count ORDER BY count DESC LIMIT 1";
        StatementResult result = session.run(statement);
        session.close();

        return result;
    }

    @Benchmark(iteration = 10)
    public StatementResult findNodeWithMostIngoingAndOutgoingEdges() {
        Session session = driver.session();
        String statement = "MATCH (p:Person)-[r:FRIEND]-(:Person) RETURN p.nodeId, count(r) as count ORDER BY count DESC LIMIT 1";
        StatementResult result = session.run(statement);
        session.close();

        return result;
    }

    @Benchmark(iteration = 10)
    public StatementResult findFriendsOfLeastConnectedNode() {
        Session session = driver.session();
        String statement = "MATCH (p:Person)-[r:FRIEND]->(friend:Person) WHERE p.nodeId = 891 RETURN friend";
        StatementResult result = session.run(statement);
        session.close();

        return result;
    }

    @Benchmark(iteration = 10)
    public StatementResult findFriendsOfAFriendsOfLeastConnectedNode() {
        Session session = driver.session();
        String statement = "MATCH (p:Person)-[:FRIEND]->(friend:Person)-[:FRIEND]->(foaf:Person) WHERE p.nodeId = 891 RETURN foaf";
        StatementResult result = session.run(statement);
        session.close();

        return result;
    }

    @Benchmark(iteration = 10)
    public StatementResult findFriendsOfMostConnectedNode() {
        Session session = driver.session();
        String statement = "MATCH (p:Person)-[r:FRIEND]->(friend:Person) WHERE p.nodeId = 107 RETURN friend";
        StatementResult result = session.run(statement);
        session.close();

        return result;
    }

    @Benchmark(iteration = 10)
    public StatementResult findFriendsOfFriendsOfMostConnectedNode() {
        Session session = driver.session();
        String statement = "MATCH (p:Person)-[:FRIEND]->(friend:Person)-[:FRIEND]->(foaf:Person) WHERE p.nodeId = 107 RETURN foaf";
        StatementResult result = session.run(statement);
        session.close();

        return result;
    }

    @Benchmark(iteration = 1, priority = 90)
    public StatementResult createNewNodeWithNodeId10000() {
        Session session = driver.session();
        String statement = "CREATE (p:Person { nodeId: 10000}) RETURN p";
        StatementResult result = session.run(statement);
        session.close();

        return result;
    }

    @Benchmark(iteration = 1, priority = 80)
    public StatementResult createNewRelationshipBetweenMostConnectedNodeAndNodeWithNodeId10000() {
        Session session = driver.session();
        String statement = "MATCH (p1:Person), (p2:Person) WHERE p1.nodeId = 107 AND p2.nodeId = 10000 CREATE (p1)-[r1:FRIEND]->(p2) CREATE (p2)-[r2:FRIEND]->(p1) RETURN r1, r2";
        StatementResult result = session.run(statement);
        session.close();

        return result;
    }

    @Benchmark(iteration = 10, priority = 70)
    public StatementResult findNodeWithNodeId10000() {
        Session session = driver.session();
        String statement = "MATCH (p:Person { nodeId:10000 }) RETURN p";
        StatementResult result = session.run(statement);
        session.close();

        return result;
    }

    @Benchmark(iteration = 10, priority = 60)
    public StatementResult updateNodeWithNodeId10000() {
        Session session = driver.session();
        String statement = "MATCH (p:Person { nodeId:10000 }) SET p.firstName = 'John', p.lastName = 'Doe' RETURN p";
        StatementResult result = session.run(statement);
        session.close();

        return result;
    }

    @Benchmark(iteration = 1, priority = 50)
    public StatementResult deleteNodeWithNodeId10000() {
        Session session = driver.session();
        String statement = "MATCH (p:Person { nodeId:10000 }) DETACH DELETE p";
        StatementResult result = session.run(statement);
        session.close();

        return result;
    }

    @Benchmark(iteration = 1, priority = 90)
    public StatementResult createNewNodeWithNodeId20000() {
        Session session = driver.session();
        String statement = "CREATE (p:Person { nodeId: 20000}) RETURN p";
        StatementResult result = session.run(statement);
        session.close();

        return result;
    }

    @Benchmark(iteration = 1, priority = 80)
    public StatementResult createNewRelationshipBetweenLeastConnectedNodeAndNodeWithNodeId20000() {
        Session session = driver.session();
        String statement = "MATCH (p1:Person), (p2:Person) WHERE p1.nodeId = 891 AND p2.nodeId = 20000 CREATE (p1)-[r1:FRIEND]->(p2) CREATE (p2)-[r2:FRIEND]->(p1) RETURN r1, r2";
        StatementResult result = session.run(statement);
        session.close();

        return result;
    }

    @Benchmark(iteration = 10, priority = 70)
    public StatementResult findNodeWithNodeId20000() {
        Session session = driver.session();
        String statement = "MATCH (p:Person { nodeId:20000 }) RETURN p";
        StatementResult result = session.run(statement);
        session.close();

        return result;
    }

    @Benchmark(iteration = 10, priority = 60)
    public StatementResult updateNodeWithNodeId20000() {
        Session session = driver.session();
        String statement = "MATCH (p:Person { nodeId:20000 }) SET p.firstName = 'John', p.lastName = 'Doe' RETURN p";
        StatementResult result = session.run(statement);
        session.close();

        return result;
    }

    @Benchmark(iteration = 1, priority = 50)
    public StatementResult deleteNodeWithNodeId20000() {
        Session session = driver.session();
        String statement = "MATCH (p:Person { nodeId:20000 }) DETACH DELETE p";
        StatementResult result = session.run(statement);
        session.close();

        return result;
    }

    @TearDown
    public void closeConnection() {
        driver.close();
    }

    public static void main(String[] args) {
        BenchmarkTest benchmarkTest = new BenchmarkTest(Neo4jBenchmark.class);
        benchmarkTest.run();
    }
}
