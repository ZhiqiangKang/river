package com.xiwei.river.vo;

public enum DataTypeInfo {

    BYTE("byte"),
    SHORT("short"),
    INTEGER("int"),
    LONG("long"),
    BOOLEAN("boolean"),
    STRING("string"),
    FLOAT("float"),
    DOUBLE("double"),
    DATE("date"),
    UNDEFINED("undefined");

    String name;

    DataTypeInfo(String name) {
        this.name = name;
    }

    public static DataTypeInfo getByName(String name) {
        for(DataTypeInfo dataTypeInfo : DataTypeInfo.values()) {
            if(dataTypeInfo.name.equalsIgnoreCase(name)) {
                return dataTypeInfo;
            }
        }
        return UNDEFINED;
    }
}
