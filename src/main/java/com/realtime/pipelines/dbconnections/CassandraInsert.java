package com.realtime.pipelines.dbconnections;

import com.datastax.driver.core.*;
import org.json.JSONObject;
import com.realtime.utils.FilterEngineToCassandra;

public class CassandraInsert {

    private final Session session;
    private PreparedStatement preparedStatement;
    private final FilterEngineToCassandra filterEngine;

    public CassandraInsert(String cassandraNodes, int cassandraPort, String cassandraKeyspace, String cassandraTable, FilterEngineToCassandra filterEngine) {
        this.filterEngine = filterEngine;

        Cluster cluster = Cluster.builder()
                .withoutJMXReporting()
                .addContactPoint(cassandraNodes)
                .withPort(cassandraPort)
                .build();

        this.session = cluster.connect(cassandraKeyspace);
        prepareStatement(cassandraTable);
    }

    private void prepareStatement(String cassandraTable) {
        String query = filterEngine.getInsertQuery(cassandraTable);
        this.preparedStatement = session.prepare(query);
    }

    public void insert(JSONObject data) {
        // Asumiendo que el orden en el PreparedStatement coincide con el orden de los campos en el archivo de configuración
        Object[] values = new Object[data.length()];
        int i = 0;
        for (String key : data.keySet()) {
            values[i++] = data.get(key);
        }
        session.execute(preparedStatement.bind(values));
    }

    public void close() {
        if (this.session != null) {
            this.session.getCluster().close();
            this.session.close();
        }
    }
}
