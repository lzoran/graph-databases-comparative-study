package com.graphdatabases.arangodb;

import com.arangodb.ArangoCursor;
import com.arangodb.ArangoDB;
import com.arangodb.ArangoDatabase;
import com.arangodb.entity.EdgeDefinition;
import com.arangodb.model.HashIndexOptions;
import com.graphdatabases.arangodb.model.Friend;
import com.graphdatabases.arangodb.model.Person;
import com.graphdatabases.benchmark.BenchmarkTest;
import com.graphdatabases.benchmark.annotation.Benchmark;
import com.graphdatabases.benchmark.annotation.Setup;
import com.graphdatabases.benchmark.annotation.TearDown;

import java.io.*;
import java.util.*;

public class ArangoDBBenchmark {

    private ArangoDB arangoDB;
    private static final String DB_NAME = "social-networks";
    private static final String GRAPH_NAME = "graph";
    private static final String EDGE_COLLECTION_NAME = "Friends";
    private static final String VERTEXT_COLLECTION_NAME = "Persons";

    @Setup
    public void setup() {

        System.out.println("Initializing database driver.");
        initializeDriver();

        System.out.println("Cleaning database.");
        clean();

        System.out.println("Initializing graph and collections.");
        initializeDatabase();

        System.out.println("Setting initial data.");
        setupInitialData();
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

    private void clean() {
        ArangoDatabase db = arangoDB.db(DB_NAME);
        if (db.exists()) {
            db.drop();
        }
    }

    private void initializeDatabase() {
        arangoDB.createDatabase(DB_NAME);

        Collection<EdgeDefinition> edgeDefinitions = new ArrayList<>();
        EdgeDefinition edgeDefinition = new EdgeDefinition();
        edgeDefinition.collection(EDGE_COLLECTION_NAME);
        edgeDefinition.from(VERTEXT_COLLECTION_NAME).to(VERTEXT_COLLECTION_NAME);
        edgeDefinitions.add(edgeDefinition);

        arangoDB.db(DB_NAME).createGraph(GRAPH_NAME, edgeDefinitions, null);
        arangoDB.db(DB_NAME).collection(VERTEXT_COLLECTION_NAME).ensureHashIndex(Collections.singletonList("nodeId"), new HashIndexOptions().unique(false));
    }

    private void setupInitialData() {
        String path = ArangoDBBenchmark.class.getResource("/datasets/facebook/facebook_combined.txt/facebook_combined.txt").getPath();
        try {
            BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(path)));
            String line;
            while ((line = br.readLine()) != null) {
                String[] nodes = line.trim().split(" ");
                if (nodes.length != 2) {
                    System.out.println(String.format("Invalid data detected: [%s]", line));
                    continue;
                }

                String nodeOneId = nodes[0];
                String nodeTwoId = nodes[1];

                Person personOne = findPersonByNodeId(nodeOneId);
                if (personOne == null) {
                    personOne = createPerson(nodeOneId);
                }

                Person personTwo = findPersonByNodeId(nodeTwoId);
                if (personTwo == null) {
                    personTwo = createPerson(nodeTwoId);
                }

                createRelationship(personOne, personTwo);
            }
        } catch (FileNotFoundException e) {
            System.out.println("Failed to load data.");
        } catch (IOException e) {
            System.out.println(String.format("Exception: %s", e.getMessage()));
        }
    }

    private Person findPersonByNodeId(String nodeId) {
        Person person = null;

        String query = "FOR p IN Persons FILTER p.`nodeId` == @nodeId RETURN p";
        Map<String, Object> vars = new HashMap<>();
        vars.put("nodeId", nodeId);
        ArangoCursor<Person> cursor = arangoDB.db(DB_NAME).query(query, vars, null, Person.class);
        if (cursor.hasNext()) {
            person = cursor.next();
        }

        return person;
    }

    private Person createPerson(String nodeId) {
        Person person = new Person(nodeId);
        arangoDB.db(DB_NAME).graph(GRAPH_NAME).vertexCollection(VERTEXT_COLLECTION_NAME).insertVertex(person);

        return person;
    }

    private void createRelationship(Person personOne, Person personTwo) {
        arangoDB.db(DB_NAME).graph(GRAPH_NAME).edgeCollection(EDGE_COLLECTION_NAME).insertEdge(new Friend(personOne.getId(), personTwo.getId()));
        arangoDB.db(DB_NAME).graph(GRAPH_NAME).edgeCollection(EDGE_COLLECTION_NAME).insertEdge(new Friend(personTwo.getId(), personOne.getId()));
    }

    @Benchmark(iteration = 10)
    public Person findNodeWithLeastIngoingEdges() {
        Person person = null;

        String query = "FOR f IN Friends COLLECT personId = f._to WITH COUNT INTO counter SORT counter ASC LIMIT 1 RETURN (FOR p IN Persons FILTER p._id == personId RETURN p)[0]";
        ArangoCursor<Person> cursor = arangoDB.db(DB_NAME).query(query, null, null, Person.class);
        if (cursor.hasNext()) {
            person = cursor.next();
        }

        return person;
    }

    @Benchmark(iteration = 10)
    public Person findNodeWithLeastOutgoingEdges() {
        Person person = null;

        String query = "FOR f IN Friends COLLECT personId = f._from WITH COUNT INTO counter SORT counter ASC LIMIT 1 RETURN (FOR p IN Persons FILTER p._id == personId RETURN p)[0]";
        ArangoCursor<Person> cursor = arangoDB.db(DB_NAME).query(query, null, null, Person.class);
        if (cursor.hasNext()) {
            person = cursor.next();
        }

        return person;
    }

    @Benchmark(iteration = 10)
    public Person findNodeWithLeastIngoingAndOutgoingEdges() {
        Person person = null;

        String query = "FOR p IN Persons LET counter = LENGTH(FOR f IN Friends FILTER f._to == p._id || f._from == p._id RETURN f) SORT counter ASC LIMIT 1 RETURN p";
        ArangoCursor<Person> cursor = arangoDB.db(DB_NAME).query(query, null, null, Person.class);
        if (cursor.hasNext()) {
            person = cursor.next();
        }

        return person;
    }

    @Benchmark(iteration = 10)
    public Person findNodeWithMostIngoingEdges() {
        Person person = null;

        String query = "FOR f IN Friends COLLECT personId = f._to WITH COUNT INTO counter SORT counter DESC LIMIT 1 RETURN (FOR p IN Persons FILTER p._id == personId RETURN p)[0]";
        ArangoCursor<Person> cursor = arangoDB.db(DB_NAME).query(query, null, null, Person.class);
        if (cursor.hasNext()) {
            person = cursor.next();
        }

        return person;
    }

    @Benchmark(iteration = 10)
    public Person findNodeWithMostOutgoingEdges() {
        Person person = null;

        String query = "FOR f IN Friends COLLECT personId = f._from WITH COUNT INTO counter SORT counter DESC LIMIT 1 RETURN (FOR p IN Persons FILTER p._id == personId RETURN p)[0]";
        ArangoCursor<Person> cursor = arangoDB.db(DB_NAME).query(query, null, null, Person.class);
        if (cursor.hasNext()) {
            person = cursor.next();
        }

        return person;
    }

    @Benchmark(iteration = 10)
    public Person findNodeWithMostIngoingAndOutgoingEdges() {
        Person person = null;

        String query = "FOR p IN Persons LET counter = LENGTH(FOR f IN Friends FILTER f._to == p._id || f._from == p._id RETURN f) SORT counter DESC LIMIT 1 RETURN p";
        ArangoCursor<Person> cursor = arangoDB.db(DB_NAME).query(query, null, null, Person.class);
        if (cursor.hasNext()) {
            person = cursor.next();
        }

        return person;
    }

    @Benchmark(iteration = 10)
    public Iterator<Person> findFriendsOfLeastConnectedNode() {
        String query = "FOR v, e, p IN 1 OUTBOUND (FOR p IN Persons FILTER p.nodeId == '891' RETURN p._id)[0] Friends RETURN v";
        ArangoCursor<Person> cursor = arangoDB.db(DB_NAME).query(query, null, null, Person.class);
        return cursor.iterator();
    }

    @Benchmark(iteration = 10)
    public Iterator<Person> findFriendsOfAFriendsOfLeastConnectedNode() {
        String query = "FOR v, e, p IN 2 OUTBOUND (FOR p IN Persons FILTER p.nodeId == '891' RETURN p._id)[0] Friends RETURN v";
        ArangoCursor<Person> cursor = arangoDB.db(DB_NAME).query(query, null, null, Person.class);
        return cursor.iterator();
    }

    @Benchmark(iteration = 10)
    public Iterator<Person> findFriendsOfMostConnectedNode() {
        String query = "FOR v, e, p IN 1 OUTBOUND (FOR p IN Persons FILTER p.nodeId == '107' RETURN p._id)[0] Friends RETURN v";
        ArangoCursor<Person> cursor = arangoDB.db(DB_NAME).query(query, null, null, Person.class);
        return cursor.iterator();
    }

    @Benchmark(iteration = 10)
    public Iterator<Person> findFriendsOfFriendsOfMostConnectedNode() {
        String query = "FOR v, e, p IN 2 OUTBOUND (FOR p IN Persons FILTER p.nodeId == '107' RETURN p._id)[0] Friends RETURN v";
        ArangoCursor<Person> cursor = arangoDB.db(DB_NAME).query(query, null, null, Person.class);
        return cursor.iterator();
    }

    @Benchmark(iteration = 1, priority = 90)
    public Person createNewNodeWithNodeId10000() {
        Person person = new Person("10000");
        arangoDB.db(DB_NAME).graph(GRAPH_NAME).vertexCollection(VERTEXT_COLLECTION_NAME).insertVertex(person);

        return person;
    }

    @Benchmark(iteration = 1, priority = 80)
    public boolean createNewRelationshipBetweenMostConnectedNodeAndNodeWithNodeId10000() {
        boolean modified = false;

        String query = "LET firstNodeId = (FOR p IN Persons FILTER p.nodeId == @firstNodeId RETURN p._id)[0] LET secondNodeId = (FOR p IN Persons FILTER p.nodeId == @secondNodeId RETURN p._id)[0] LET friends = [{ _from: firstNodeId, _to: secondNodeId }, { _from: secondNodeId, _to: firstNodeId }] FOR f IN friends INSERT f IN Friends LET inserted = NEW RETURN inserted";
        Map<String, Object> vars = new HashMap<>();
        vars.put("firstNodeId", "107");
        vars.put("secondNodeId", "10000");
        ArangoCursor<Friend> cursor = arangoDB.db(DB_NAME).query(query, vars, null, Friend.class);

        modified = cursor.hasNext();

        return modified;
    }

    @Benchmark(iteration = 10, priority = 70)
    public Person findNodeWithNodeId10000() {
        Person person = null;

        String query = "FOR p IN Persons FILTER p.`nodeId` == @nodeId RETURN p";
        Map<String, Object> vars = new HashMap<>();
        vars.put("nodeId", "10000");
        ArangoCursor<Person> cursor = arangoDB.db(DB_NAME).query(query, vars, null, Person.class);
        if (cursor.hasNext()) {
            person = cursor.next();
        }

        return person;
    }

    @Benchmark(iteration = 10, priority = 60)
    public Person updateNodeWithNodeId10000() {
        Person person = null;

        String query = "FOR p IN Persons FILTER p.`nodeId` == @nodeId UPDATE p WITH {firstName: \"John\", lastName: \"Doe\"} IN Persons RETURN NEW";
        Map<String, Object> vars = new HashMap<>();
        vars.put("nodeId", "10000");
        ArangoCursor<Person> cursor = arangoDB.db(DB_NAME).query(query, vars, null, Person.class);
        if (cursor.hasNext()) {
            person = cursor.next();
        }

        return person;
    }

    @Benchmark(iteration = 1, priority = 50)
    public Person deleteNodeWithNodeId10000() {
        Person person = null;

        String query = "FOR p IN Persons FILTER p.`nodeId` == @nodeId REMOVE p IN Persons RETURN OLD";
        Map<String, Object> vars = new HashMap<>();
        vars.put("nodeId", "10000");
        ArangoCursor<Person> cursor = arangoDB.db(DB_NAME).query(query, vars, null, Person.class);
        if (cursor.hasNext()) {
            person = cursor.next();
        }

        return person;
    }

    @Benchmark(iteration = 1, priority = 90)
    public Person createNewNodeWithNodeId20000() {
        Person person = new Person("20000");
        arangoDB.db(DB_NAME).graph(GRAPH_NAME).vertexCollection(VERTEXT_COLLECTION_NAME).insertVertex(person);

        return person;
    }

    @Benchmark(iteration = 1, priority = 80)
    public boolean createNewRelationshipBetweenLeastConnectedNodeAndNodeWithNodeId20000() {
        boolean modified = false;

        String query = "LET firstNodeId = (FOR p IN Persons FILTER p.nodeId == @firstNodeId RETURN p._id)[0] LET secondNodeId = (FOR p IN Persons FILTER p.nodeId == @secondNodeId RETURN p._id)[0] LET friends = [{ _from: firstNodeId, _to: secondNodeId }, { _from: secondNodeId, _to: firstNodeId }] FOR f IN friends INSERT f IN Friends LET inserted = NEW RETURN inserted";
        Map<String, Object> vars = new HashMap<>();
        vars.put("firstNodeId", "891");
        vars.put("secondNodeId", "20000");
        ArangoCursor<Friend> cursor = arangoDB.db(DB_NAME).query(query, vars, null, Friend.class);

        modified = cursor.hasNext();

        return modified;
    }

    @Benchmark(iteration = 10, priority = 70)
    public Person findNodeWithNodeId20000() {
        Person person = null;

        String query = "FOR p IN Persons FILTER p.`nodeId` == @nodeId RETURN p";
        Map<String, Object> vars = new HashMap<>();
        vars.put("nodeId", "20000");
        ArangoCursor<Person> cursor = arangoDB.db(DB_NAME).query(query, vars, null, Person.class);
        if (cursor.hasNext()) {
            person = cursor.next();
        }

        return person;
    }

    @Benchmark(iteration = 10, priority = 60)
    public Person updateNodeWithNodeId20000() {
        Person person = null;

        String query = "FOR p IN Persons FILTER p.`nodeId` == @nodeId UPDATE p WITH {firstName: \"John\", lastName: \"Doe\"} IN Persons RETURN NEW";
        Map<String, Object> vars = new HashMap<>();
        vars.put("nodeId", "20000");
        ArangoCursor<Person> cursor = arangoDB.db(DB_NAME).query(query, vars, null, Person.class);
        if (cursor.hasNext()) {
            person = cursor.next();
        }

        return person;
    }

    @Benchmark(iteration = 1, priority = 50)
    public Person deleteNodeWithNodeId20000() {
        Person person = null;

        String query = "FOR p IN Persons FILTER p.`nodeId` == @nodeId REMOVE p IN Persons RETURN OLD";
        Map<String, Object> vars = new HashMap<>();
        vars.put("nodeId", "20000");
        ArangoCursor<Person> cursor = arangoDB.db(DB_NAME).query(query, vars, null, Person.class);
        if (cursor.hasNext()) {
            person = cursor.next();
        }

        return person;
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
