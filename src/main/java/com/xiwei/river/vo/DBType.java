package com.xiwei.river.vo;

public enum DBType {

    REDIS("redis"),
    HBASE("hbase"),
    UNDEFINED("undefined");

    private String name;

    DBType(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public static DBType getByName(String name) {
        for(DBType dbType : DBType.values()) {
            if(dbType.getName().equalsIgnoreCase(name)) {
                return dbType;
            }
        }
        return UNDEFINED;
    }
}
