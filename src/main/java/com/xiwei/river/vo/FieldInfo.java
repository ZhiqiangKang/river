package com.xiwei.river.vo;

import java.io.Serializable;

public class FieldInfo implements Serializable{

    private String name;
    private String datePattern;
    private String newName;
    private String newDatePattern;
    private String newType;
    private DataTypeInfo newDataTypeInfo = DataTypeInfo.UNDEFINED;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getDatePattern() {
        return datePattern;
    }

    public void setDatePattern(String datePattern) {
        this.datePattern = datePattern;
    }

    public String getNewName() {
        return newName;
    }

    public void setNewName(String newName) {
        this.newName = newName;
    }

    public String getNewDatePattern() {
        return newDatePattern;
    }

    public void setNewDatePattern(String newDatePattern) {
        this.newDatePattern = newDatePattern;
    }

    public String getNewType() {
        return newType;
    }

    public void setNewType(String newType) {
        this.newType = newType;
        this.newDataTypeInfo = DataTypeInfo.getByName(newType);
    }

    public DataTypeInfo getNewDataTypeInfo() {
        return newDataTypeInfo;
    }

    public void setNewDataTypeInfo(DataTypeInfo newDataTypeInfo) {
        this.newDataTypeInfo = newDataTypeInfo;
    }
}
