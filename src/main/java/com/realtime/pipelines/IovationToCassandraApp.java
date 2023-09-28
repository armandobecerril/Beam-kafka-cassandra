package com.realtime.pipelines;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import com.realtime.pipelines.dbconnections.CassandraConnection;
import com.realtime.utils.FilterEngineToCassandra;
import com.realtime.utils.JsonUtils;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.json.JSONArray;
import org.json.JSONObject;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IovationToCassandraApp {

        private static final Logger log = LoggerFactory.getLogger(IovationToCassandraApp.class);
    // Define una interfaz para las opciones personalizadas de tu Pipeline.
    public interface IovationOptions extends PipelineOptions {
        @Description("Path to the config file")
        @Default.String("/config/data_pipeline_config.json")
        String getConfigFile();

        void setConfigFile(String value);
    }

    public static void main(String[] args) throws Exception {
        PipelineOptionsFactory.register(IovationOptions.class);
        IovationOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(IovationOptions.class);

        String configFile = options.getConfigFile();

        // Log de la configuración utilizada
        if (configFile.equals("/config/data_pipeline_config.json")) {
            log.info("Utilizando configuración por defecto desde: {}", configFile);
        } else {
            log.info("Utilizando configuración personalizada desde: {}", configFile);
        }
        // Creación del pipeline
        Pipeline p = Pipeline.create(options);
        runCassandraIngestionTask(p, options.getConfigFile());
        p.run().waitUntilFinish();
    }

    private static void runCassandraIngestionTask(Pipeline p, String configFile) throws Exception {
        JSONObject configJson = JsonUtils.readConfig(configFile);
        FilterEngineToCassandra filterEngine = new FilterEngineToCassandra(configJson);

        // Definición de parámetros
        String BOOTSTRAP_SERVERS = "54.89.134.122:9092";
        String INPUT_TOPIC = filterEngine.getInputTopic();
        String CASSANDRA_NODES = filterEngine.getCassandraNodes();
        String CASSANDRA_KEYSPACE = filterEngine.getCassandraKeyspace();
        String CASSANDRA_TABLE = filterEngine.getCassandraTable();

        // Leer registros de Kafka
        // PCollection es un conjunto de datos distribuido que puede ser procesado por Apache Beam.
        PCollection<String> kafkaRecords = p.apply("ReadFromKafka", KafkaIO.<String, String>read()
                        .withBootstrapServers(BOOTSTRAP_SERVERS)
                        .withTopic(INPUT_TOPIC)
                        .withKeyDeserializer(StringDeserializer.class)
                        .withValueDeserializer(StringDeserializer.class)
                        .withConsumerConfigUpdates(getKafkaConfig())
                        .withoutMetadata())
                .apply(MapElements
                        .into(TypeDescriptor.of(String.class))
                        .via((SerializableFunction<KV<String, String>, String>) record -> record.getValue()));

        // Filtrar registros usando FilterAndTransformFn
        String configJsonString = configJson.toString();
        PCollection<String> filteredKafkaRecords = kafkaRecords.apply("FilterFields",
                ParDo.of(new FilterAndTransformFn(configJsonString)));

        // Escribir registros filtrados a Cassandra usando WriteToCassandraFn
        filteredKafkaRecords.apply("WriteToCassandra",
                ParDo.of(new WriteToCassandraFn(CASSANDRA_NODES, CASSANDRA_KEYSPACE, CASSANDRA_TABLE, configJsonString)));

    }

    private static Map<String, Object> getKafkaConfig() {
        Map<String, Object> kafkaConfig = new HashMap<>();
        kafkaConfig.put("auto.offset.reset", "latest");
        kafkaConfig.put("enable.auto.commit", false);
        return kafkaConfig;
    }

    // DoFn es una función que transforma elementos en un PCollection.
    // Esta DoFn específica filtra y transforma los registros de Kafka.
    static class FilterAndTransformFn extends DoFn<String, String> {
        private final String configJsonString;
        private transient FilterEngineToCassandra filterEngine;

        public FilterAndTransformFn(String configJsonString) {
            this.configJsonString = configJsonString;
        }
        @Setup
        public void setup() {
            JSONObject configJson = new JSONObject(configJsonString);
            filterEngine = new FilterEngineToCassandra(configJson);
        }
        @ProcessElement
        public void processElement(ProcessContext c) {
            String jsonMessage = c.element();
            JSONObject message = new JSONObject(jsonMessage);
            JSONObject filteredMessage = filterEngine.filter(message);
            if (filteredMessage != null) {
                //log.info("MENSAJE filtrado directamente: " + message.toString());
                // Log para mostrar el mensaje filtrado
                c.output(filteredMessage.toString());
                log.info("MENSAJE después de filtrar: " + filteredMessage.toString());

            }
        }
    }

    // Esta DoFn específica escribe registros filtrados en Cassandra.
    static class WriteToCassandraFn extends DoFn<String, Void> {
        private final String cassandraNodes;
        private final String cassandraKeyspace;
        private final String cassandraTable;

        private transient CassandraConnection cassandraConnection;
        private transient Session session;
        private transient PreparedStatement preparedStatement;

        private String configJsonString;

        public WriteToCassandraFn(String cassandraNodes, String cassandraKeyspace, String cassandraTable, String configJsonString) {
            this.cassandraNodes = cassandraNodes;
            this.cassandraKeyspace = cassandraKeyspace;
            this.cassandraTable = cassandraTable;
            this.configJsonString = configJsonString;
        }

        private JSONObject getConfigJson() {
            return new JSONObject(configJsonString);
        }

        @Setup
        public void setup() {
            cassandraConnection = new CassandraConnection("54.152.221.55", 9042);
            session = cassandraConnection.getSession();

            StringBuilder queryFields = new StringBuilder();
            StringBuilder queryValues = new StringBuilder();

            JSONObject tableSchema = getConfigJson().getJSONObject("tableSchema");
            for (String field : tableSchema.keySet()) {
                queryFields.append(field).append(", ");
                queryValues.append("?, ");
            }

            // Remover las comas y espacios adicionales al final
            queryFields.setLength(queryFields.length() - 2);
            queryValues.setLength(queryValues.length() - 2);

            String query = "INSERT INTO " + cassandraKeyspace + "." + cassandraTable + "(" + queryFields.toString() + ") " +
                    "VALUES (" + queryValues.toString() + ")";

            log.info("*****INSERT TO CASSANDRA: {}", query);

            preparedStatement = session.prepare(new SimpleStatement(query));
        }

        @ProcessElement
        public void processElement(ProcessContext c) {
            JSONObject filteredMessage = new JSONObject(c.element());
            log.info("Mensaje JSON procesado para Ingesta: {}", filteredMessage.toString());

            BoundStatement boundStatement = preparedStatement.bind();

            // Mapeo del mensaje filtrado a los campos de Cassandra.
            for (String field : getConfigJson().getJSONObject("tableSchema").keySet()) {
                Object value = filteredMessage.opt(field);
                if (value != null) {
                    switch (getConfigJson().getJSONObject("tableSchema").getString(field)) {
                        case "UUID":
                            boundStatement = boundStatement.setUUID(field, UUID.fromString((String) value));
                            break;
                        case "TEXT":
                            boundStatement = boundStatement.setString(field, (String) value);
                            break;
                        case "INET":
                            try {
                                InetAddress addressValue = InetAddress.getByName((String) value);
                                boundStatement = boundStatement.setInet(field, addressValue);
                            } catch (UnknownHostException e) {
                                log.error("Error al convertir la dirección IP: ", e);
                            }
                            break;
                    }
                }
            }

            try {
                log.info("Ejecutando INSERT en Cassandra: {}", boundStatement.toString()); // Log para mostrar la operación de INSERT
                // Ahora ejecuta el boundStatement
                session.execute(boundStatement);
            } catch (Exception e) {
                log.error("Error al procesar y/o insertar el mensaje en Cassandra: ", e);
            }
        }


        @Teardown
        public void teardown() {
            if (cassandraConnection != null) {
                cassandraConnection.close();
            }
        }
    }
}