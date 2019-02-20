package com.xiwei.river.vo;

import com.xiwei.river.util.JsonUtil;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.util.List;

public class TableInfo {

    private String sql;
    private String srcTable;
    private String destTable;
    private String key;
    private List<FieldInfo> fields;

    /**
     * 读取JSON配置文件并转换为TableInfo对象
     * @param tableInfoFilePath 表结构及数据文件信息配置文件路径
     * @return
     * @throws IOException
     */
    public static TableInfo convertFromFile(String tableInfoFilePath) throws IOException {
        String fileContent = FileUtils.readFileToString(new File(tableInfoFilePath));
        TableInfo tableInfo = JsonUtil.toObject(fileContent, TableInfo.class);

        return tableInfo;
    }

    public String getSql() {
        return sql;
    }

    public void setSql(String sql) {
        this.sql = sql;
    }

    public String getSrcTable() {
        return srcTable;
    }

    public void setSrcTable(String srcTable) {
        this.srcTable = srcTable;
    }

    public String getDestTable() {
        return destTable;
    }

    public void setDestTable(String destTable) {
        this.destTable = destTable;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public List<FieldInfo> getFields() {
        return fields;
    }

    public void setFields(List<FieldInfo> fields) {
        this.fields = fields;
    }
}
