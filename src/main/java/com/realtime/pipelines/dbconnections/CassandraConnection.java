package com.realtime.pipelines.dbconnections;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;

public class CassandraConnection implements AutoCloseable {
    private final Cluster cluster;
    private final Session session;

    public CassandraConnection(String contactPoint, int port) {
        this.cluster = Cluster.builder()
                .withoutJMXReporting()
                .addContactPoint(contactPoint)
                .withPort(port)
                .build();
        this.session = cluster.connect();
    }

    public Session getSession() {
        return session;
    }

    @Override
    public void close() {
        if (cluster != null) {
            cluster.close();
        }
    }
}
