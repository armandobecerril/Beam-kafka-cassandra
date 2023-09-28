package com.realtime.utils;

import com.realtime.pipelines.IovationToCassandraApp;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
public class FilterEngineToCassandra implements Serializable {

    private static final Logger log = LoggerFactory.getLogger(FilterEngineToCassandra.class);

    private JSONObject config;
    private String inputTopic;
    private String cassandraNodes;
    private String cassandraKeyspace;
    private String cassandraTable;
    private List<String> fields;

    public FilterEngineToCassandra(JSONObject configJson) {

        this.config = configJson;

        this.inputTopic = config.getString("inputTopic");
        this.cassandraNodes = config.getString("cassandraNodes");
        this.cassandraKeyspace = config.getString("cassandraKeyspace");
        this.cassandraTable = config.getString("cassandraTable");

        JSONArray fieldsArray = config.getJSONArray("fields");
        this.fields = new ArrayList<>();
        for (int i = 0; i < fieldsArray.length(); i++) {
            this.fields.add(fieldsArray.getString(i));
        }
    }

    public String getInsertQuery(String cassandraTable) {
        StringBuilder queryFields = new StringBuilder();
        StringBuilder queryValues = new StringBuilder();

        JSONObject tableSchema = config.getJSONObject("tableSchema");
        for (String field : tableSchema.keySet()) {
            queryFields.append(field).append(", ");
            queryValues.append("?, ");
        }

        // Remover las comas y espacios adicionales al final
        queryFields.setLength(queryFields.length() - 2);
        queryValues.setLength(queryValues.length() - 2);

        return "INSERT INTO " + cassandraTable + "(" + queryFields.toString() + ") " +
                "VALUES (" + queryValues.toString() + ")";
    }

    public String getInputTopic() {
        return inputTopic;
    }

    public String getCassandraNodes() {
        return cassandraNodes;
    }

    public String getCassandraKeyspace() {
        return cassandraKeyspace;
    }

    public String getCassandraTable() {
        return cassandraTable;
    }

    public List<String> getFields() {
        return fields;
    }

    public JSONObject filter(JSONObject message) {
        log.info("Filtrando mensaje: " + message.toString());
        JSONObject filteredMessage = new JSONObject();

        // Iterar sobre los campos deseados desde la configuración y extraerlos del mensaje
        for (String fieldPath : fields) {
            Object value = getValueFromPath(message, fieldPath);
            if (value != null) {
                String[] fieldParts = fieldPath.split("\\.");
                filteredMessage.put(fieldParts[fieldParts.length - 1], value);
            }
        }

        return filteredMessage;
    }


    private Object getValueFromPath(JSONObject json, String path) {
        log.info("Buscando ruta: " + path);

        String[] parts = path.split("\\.");
        Object obj = json;
        for (String part : parts) {
            log.info("Procesando parte: " + part);

            if (obj instanceof JSONObject) {
                obj = ((JSONObject) obj).opt(part);
                if (obj == null) {
                    log.warn("Parte '" + part + "' no encontrada en el JSON.");
                    return null;  // Devolver null si no se encuentra la parte en el JSON
                } else {
                    log.info("Valor para la parte '" + part + "': " + obj.toString());
                }
            } else {
                log.warn("El objeto no es un JSONObject, es un: " + obj.getClass().getSimpleName());
                return null;  // Devolver null si el objeto no es un JSONObject
            }
        }
        return obj;
    }
}
