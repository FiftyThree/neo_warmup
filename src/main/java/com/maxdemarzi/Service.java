package com.maxdemarzi;

import org.codehaus.jackson.map.ObjectMapper;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.tooling.GlobalGraphOperations;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Path("/service")
public class Service {

    private static final ObjectMapper objectMapper = new ObjectMapper();
    private volatile long nodeCount = 0;
    private volatile long relCount = 0;

    @GET
    @Path("/helloworld")
    public Response helloWorld() throws IOException {
        Map<String, String> results = new HashMap<String,String>(){{
            put("response","hello world");
        }};
        return Response.ok().entity(objectMapper.writeValueAsString(results)).build();
    }

    @GET
    @Path("/warmup")
    public Response warmUp(final @Context GraphDatabaseService db) throws IOException {

        ExecutorService service = Executors.newCachedThreadPool();

        Callable<Void> readNodes = new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                try (Transaction tx = db.beginTx()) {
                    for (Node node : GlobalGraphOperations.at(db).getAllNodes()) {
                        node.getPropertyKeys();
                        nodeCount++;
                    }
                }
                return null;
            }
        };

        Callable<Void> readRels = new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                try (Transaction tx = db.beginTx()) {
                    for (Relationship relationship : GlobalGraphOperations.at(db).getAllRelationships()) {
                        relationship.getPropertyKeys();
                        relationship.getNodes();
                        relCount++;
                    }
                }
                return null;
            }
        };

        final Map<String, String> results = new HashMap<>();

        try {
            service.invokeAll(Arrays.asList(readNodes, readRels));
            results.put("response", "Warmed up and ready to go with " + nodeCount + " nodes and " + relCount + " rels");
        } catch (Exception e) {
            results.put("response", "Error:" + e.getMessage());
        }

        return Response.ok().entity(objectMapper.writeValueAsString(results)).build();
    }

}
