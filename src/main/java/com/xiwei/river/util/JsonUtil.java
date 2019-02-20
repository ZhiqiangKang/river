package com.xiwei.river.util;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

import java.io.File;
import java.io.IOException;
import java.util.List;

public class JsonUtil {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    static {
        OBJECT_MAPPER.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
        OBJECT_MAPPER.disable(SerializationFeature.WRITE_NULL_MAP_VALUES);
        OBJECT_MAPPER.enable(DeserializationFeature.ACCEPT_EMPTY_STRING_AS_NULL_OBJECT) ;
    }

    public static <T> List<T> toList(File file, Class<T> elementClass){
        try {
            return (List<T>)OBJECT_MAPPER.readValue(file, OBJECT_MAPPER.getTypeFactory().constructParametricType(List.class, elementClass));
        } catch (IOException e) {
            throw new RuntimeException("JsonUtil toList file:["+file.getAbsolutePath()+"] error", e);
        }
    }

    public static <T> List<T> toList(String json, Class<T> elementClass){
        try {
            return (List<T>)OBJECT_MAPPER.readValue(json, OBJECT_MAPPER.getTypeFactory().constructParametricType(List.class, elementClass));
        } catch (IOException e) {
            throw new RuntimeException("JsonUtil toList json:["+json+"] error", e);
        }
    }

    public static <T> T toObject(String json, Class<T> clazz) {
        try {
            return OBJECT_MAPPER.readValue(json, clazz);
        } catch (IOException e) {
            throw new RuntimeException("JsonUtil toObject json:["+json+"] error", e);
        }
    }

    public static String toJson(Object obj) {
        try {
            return OBJECT_MAPPER.writeValueAsString(obj);
        } catch (IOException e) {
            throw new RuntimeException("JsonUtil toJson obj:["+obj+"] error", e);
        }
    }
}
