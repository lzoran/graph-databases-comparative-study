package com.graphdatabases.arangodb.model;

import com.arangodb.entity.DocumentField;
import com.arangodb.entity.DocumentField.Type;

public class Person {

    @DocumentField(Type.ID)
    private String id;

    @DocumentField(Type.KEY)
    private String key;

    @DocumentField(Type.REV)
    private String revision;

    private String nodeId;

    public Person() {}

    public Person(String nodeId) {
        this.nodeId = nodeId;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public String getRevision() {
        return revision;
    }

    public void setRevision(String revision) {
        this.revision = revision;
    }

    public String getNodeId() {
        return nodeId;
    }

    public void setNodeId(String nodeId) {
        this.nodeId = nodeId;
    }
}
