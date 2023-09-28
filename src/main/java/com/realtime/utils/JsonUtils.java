package com.realtime.utils;

import java.io.InputStream;
import java.io.Serializable;

import org.json.JSONObject;
import org.json.JSONTokener;
public class JsonUtils {

    public static JSONObject readConfig(String path) {
        InputStream configStream = JsonUtils.class.getResourceAsStream(path);
        if (configStream == null) {
            throw new RuntimeException("Configuration file not found: " + path);
        }
        return new JSONObject(new JSONTokener(configStream));
    }

    public static String getNestedValue(JSONObject json, String[] keys) {
        JSONObject temp = json;
        for (int i = 0; i < keys.length - 1; i++) {
            temp = temp.getJSONObject(keys[i]);
        }
        return temp.getString(keys[keys.length - 1]);
    }
}
