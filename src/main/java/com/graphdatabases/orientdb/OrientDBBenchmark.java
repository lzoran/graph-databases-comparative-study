package com.graphdatabases.orientdb;

import com.graphdatabases.benchmark.BenchmarkTest;
import com.graphdatabases.benchmark.annotation.Benchmark;
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
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

public class OrientDBBenchmark {

    private OrientGraphFactory factory;

    @Setup
    public void setup() {

        System.out.println("Initializing database driver.");
        initializeDriver();

        System.out.println("Cleaning database.");
        clean();

        System.out.println("Initializing classes.");
        initializeClasses();

        System.out.println("Setting initial data.");
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

    private void initializeClasses(){
        OrientGraphNoTx graphNoTx = factory.getNoTx();

        graphNoTx.getRawGraph().command(new OCommandSQL("CREATE CLASS Person IF NOT EXISTS EXTENDS V")).execute();
        graphNoTx.getRawGraph().command(new OCommandSQL("CREATE PROPERTY Person.nodeId IF NOT EXISTS INTEGER")).execute();
        graphNoTx.getRawGraph().command(new OCommandSQL("CREATE INDEX Person.nodeId ON Person (nodeId) NOTUNIQUE")).execute();

        graphNoTx.getRawGraph().command(new OCommandSQL("CREATE CLASS Friend IF NOT EXISTS EXTENDS E")).execute();
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

    @Benchmark(iteration = 10)
    public Vertex findNodeWithLeastIngoingEdges() {
        Vertex vertex = null;

        OrientGraph graph = factory.getTx();
        try {
            Iterable<Vertex> iterable = graph.command(new OCommandSQL("SELECT nodeId, IN().size() AS count FROM Person ORDER BY count ASC LIMIT 1")).execute();
            if (iterable.iterator().hasNext()) {
                vertex = iterable.iterator().next();
            }
        } finally {
            graph.shutdown();
        }

        return vertex;
    }

    @Benchmark(iteration = 10)
    public Vertex findNodeWithLeastOutgoingEdges() {
        Vertex vertex = null;

        OrientGraph graph = factory.getTx();
        try {
            Iterable<Vertex> iterable = graph.command(new OCommandSQL("SELECT nodeId, OUT().size() AS count FROM Person ORDER BY count ASC LIMIT 1")).execute();
            if (iterable.iterator().hasNext()) {
                vertex = iterable.iterator().next();
            }
        } finally {
            graph.shutdown();
        }

        return vertex;
    }

    @Benchmark(iteration = 10)
    public Vertex findNodeWithLeastIngoingAndOutgoingEdges() {
        Vertex vertex = null;

        OrientGraph graph = factory.getTx();
        try {
            Iterable<Vertex> iterable = graph.command(new OCommandSQL("SELECT nodeId, BOTH().size() AS count FROM Person ORDER BY count ASC LIMIT 1")).execute();
            if (iterable.iterator().hasNext()) {
                vertex = iterable.iterator().next();
            }
        } finally {
            graph.shutdown();
        }

        return vertex;
    }

    @Benchmark(iteration = 10)
    public Vertex findNodeWithMostIngoingEdges() {
        Vertex vertex = null;

        OrientGraph graph = factory.getTx();
        try {
            Iterable<Vertex> iterable = graph.command(new OCommandSQL("SELECT nodeId, IN().size() AS count FROM Person ORDER BY count DESC LIMIT 1")).execute();
            if (iterable.iterator().hasNext()) {
                vertex = iterable.iterator().next();
            }
        } finally {
            graph.shutdown();
        }

        return vertex;
    }

    @Benchmark(iteration = 10)
    public Vertex findNodeWithMostOutgoingEdges() {
        Vertex vertex = null;

        OrientGraph graph = factory.getTx();
        try {
            Iterable<Vertex> iterable = graph.command(new OCommandSQL("SELECT nodeId, OUT().size() AS count FROM Person ORDER BY count DESC LIMIT 1")).execute();
            if (iterable.iterator().hasNext()) {
                vertex = iterable.iterator().next();
            }
        } finally {
            graph.shutdown();
        }

        return vertex;
    }

    @Benchmark(iteration = 10)
    public Vertex findNodeWithMostIngoingAndOutgoingEdges() {
        Vertex vertex = null;

        OrientGraph graph = factory.getTx();
        try {
            Iterable<Vertex> iterable = graph.command(new OCommandSQL("SELECT nodeId, BOTH().size() AS count FROM Person ORDER BY count DESC LIMIT 1")).execute();
            if (iterable.iterator().hasNext()) {
                vertex = iterable.iterator().next();
            }
        } finally {
            graph.shutdown();
        }

        return vertex;
    }

    @Benchmark(iteration = 10)
    public Iterator<Vertex> findFriendsOfLeastConnectedNode() {
        Iterator<Vertex> iterator = null;

        OrientGraph graph = factory.getTx();
        try {
            Iterable<Vertex> iterable = graph.command(new OCommandSQL("SELECT BOTH(\"Friend\") FROM Person WHERE nodeId = 891")).execute();
            iterator = iterable.iterator();
        } finally {
            graph.shutdown();
        }

        return iterator;
    }

    @Benchmark(iteration = 10)
    public Iterator<Vertex> findFriendsOfAFriendsOfLeastConnectedNode() {
        Iterator<Vertex> iterator = null;

        OrientGraph graph = factory.getTx();
        try {
            Iterable<Vertex> iterable = graph.command(new OCommandSQL("SELECT BOTH(\"Friend\").BOTH(\"Friend\") FROM Person WHERE nodeId = 891")).execute();
            iterator = iterable.iterator();
        } finally {
            graph.shutdown();
        }

        return iterator;
    }

    @Benchmark(iteration = 10)
    public Iterator<Vertex> findFriendsOfMostConnectedNode() {
        Iterator<Vertex> iterator = null;

        OrientGraph graph = factory.getTx();
        try {
            Iterable<Vertex> iterable = graph.command(new OCommandSQL("SELECT BOTH(\"Friend\") FROM Person WHERE nodeId = 107")).execute();
            iterator = iterable.iterator();
        } finally {
            graph.shutdown();
        }

        return iterator;
    }

    @Benchmark(iteration = 10)
    public Iterator<Vertex> findFriendsOfFriendsOfMostConnectedNode() {
        Iterator<Vertex> iterator = null;

        OrientGraph graph = factory.getTx();
        try {
            Iterable<Vertex> iterable = graph.command(new OCommandSQL("SELECT BOTH(\"Friend\").BOTH(\"Friend\") FROM Person WHERE nodeId = 107")).execute();
            iterator = iterable.iterator();
        } finally {
            graph.shutdown();
        }

        return iterator;
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
