package com.graphdatabases.orientdb;

import com.graphdatabases.benchmark.BenchmarkTest;
import com.graphdatabases.benchmark.annotation.Setup;
import com.graphdatabases.benchmark.annotation.TearDown;
import com.graphdatabases.neo4j.Neo4jBenchmark;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.tinkerpop.blueprints.*;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import com.tinkerpop.blueprints.impls.orient.OrientGraphFactory;
import com.tinkerpop.blueprints.impls.orient.OrientGraphNoTx;
import org.neo4j.driver.v1.AuthTokens;
import org.neo4j.driver.v1.GraphDatabase;

import java.io.*;
import java.util.Properties;

public class OrientDBBenchmark {

    private OrientGraphFactory factory;

    @Setup
    public void setup() {

        System.out.println("Initializing OrientDB database driver.");
        initializeDriver();

        System.out.println("Cleaning database - removing nodes and relationships between them.");
        clean();

        System.out.println("Initializing classes");
        initializeClasses();

        System.out.println("Creating index on property nodeId.");
        createIndexOnProperty("Person", "nodeId");

        System.out.println("Loading test data");
        setupInitialData();
    }

    private void initializeDriver() {
        Properties properties = new Properties();
        try {
            String path = OrientDBBenchmark.class.getResource("driver-settings.properties").getPath();
            FileInputStream in = new FileInputStream(path);
            properties.load(in);
            in.close();
        } catch (Exception e) {
            System.out.println("Failed to load driver settings.");
        }

        String uri = properties.getProperty("uri");
        String username = properties.getProperty("username");
        String password = properties.getProperty("password");

        factory = new OrientGraphFactory(uri, username, password).setupPool(1,10);
    }

    private void clean() {
        OrientGraphNoTx graphNoTx = factory.getNoTx();

        graphNoTx.getRawGraph().command(new OCommandSQL("DROP CLASS Friend IF EXISTS UNSAFE")).execute();
        graphNoTx.getRawGraph().command(new OCommandSQL("DROP CLASS Person IF EXISTS UNSAFE")).execute();
    }

    private void initializeClasses() {
        createClass("Person", "V");
        createClass("Friend", "E");
    }

    private void createClass(String className, String superClassName){
        OrientGraphNoTx graphNoTx = factory.getNoTx();
        graphNoTx.getRawGraph().command(new OCommandSQL(String.format("CREATE CLASS %s IF NOT EXISTS EXTENDS %s", className, superClassName))).execute();
    }

    private void createIndexOnProperty(String className, String propertyValue) {
        OrientGraphNoTx graphNoTx = factory.getNoTx();
        graphNoTx.getRawGraph().command(new OCommandSQL("CREATE PROPERTY Person.nodeId IF NOT EXISTS INTEGER")).execute();
        graphNoTx.getRawGraph().command(new OCommandSQL(String.format("CREATE INDEX %s.%s ON %s (%s) NOTUNIQUE", className, propertyValue, className, propertyValue))).execute();
    }

    private void setupInitialData() {
        String path = OrientDBBenchmark.class.getResource("/datasets/facebook/facebook_combined.txt/facebook_combined.txt").getPath();
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

                Vertex personOne = findPersonByNodeId(nodeOneId);
                if (personOne == null) {
                    personOne = createNode(nodeOneId);
                }

                Vertex personTwo = findPersonByNodeId(nodeTwoId);
                if (personTwo == null) {
                    personTwo = createNode(nodeTwoId);
                }

                createRelationship(personOne, personTwo);
            }
        } catch (FileNotFoundException e) {
            System.out.println("Failed to load data.");
        } catch (IOException e) {
            System.out.println(String.format("Exception: %s", e.getMessage()));
        }
    }

    public Vertex findPersonByNodeId(Integer nodeId) {
        Vertex person = null;

        OrientGraph graph = factory.getTx();
        try {
            Iterable<Vertex> iterable = graph.getVertices("Person.nodeId", nodeId);
            if (iterable.iterator().hasNext()) {
                person = iterable.iterator().next();
            }
        } finally {
            graph.shutdown();
        }

        return person;
    }

    public Vertex createNode(Integer nodeId) {
        Vertex person = null;

        OrientGraph graph = factory.getTx();
        try {
            person = graph.addVertex("class:Person");
            person.setProperty("nodeId", nodeId);

            graph.commit();
        } finally {
            graph.shutdown();
        }

        return person;
    }

    public void createRelationship(Vertex vertexOne, Vertex vertexTwo) {
        OrientGraph graph = factory.getTx();
        try {
            graph.addEdge("class:Friend", vertexOne, vertexTwo, null);
            graph.addEdge("class:Friend", vertexTwo, vertexOne, null);

            graph.commit();
        } finally {
            graph.shutdown();
        }
    }

    @TearDown
    public void tearDown() {
        factory.close();
    }

    public static void main(String[] args) {
        BenchmarkTest benchmarkTest = new BenchmarkTest(OrientDBBenchmark.class);
        benchmarkTest.run();
    }
}
