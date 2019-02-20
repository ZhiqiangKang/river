package com.xiwei.river.core;

import com.xiwei.river.vo.DBType;

public class ExeArgParser {
    private String dbTypeValue;
    private DBType dbType;
    private String configValue;
    private String redisProperties;

    public void parse(String[] args) {

        if (args.length >= 2){
            dbTypeValue = args[0];
            dbType = DBType.getByName(dbTypeValue);
            configValue = args[1];

            if (dbType.equals(DBType.UNDEFINED)){
                String msg = "<arg1>可选值为redis|hbase";
                exit(msg);
            }
            if (dbType.equals(DBType.REDIS)){
                if (args.length == 2){
                    String msg = "<arg1>为redis时，<arg3>必传";
                    exit(msg);
                } else {
                    redisProperties = args[2];
                }
            }
        } else {
            exit("请传入合法的运行参数");
        }
    }

    // 报错退出
    private static void exit(String msg){

        System.out.println("Error: " + msg);
        String usage = "Usage: <arg1> <arg2> <arg3>\n" +
                        "       arg1 - 指定数据库类型。可选：redis|hbase。必传\n" +
                        "       arg2 - 表字段配置文件路径。必传\n" +
                        "       arg3 - redis配置文件路径。<arg1>为redis时，必传";
        System.out.println(usage);

        System.exit(-1);
    }

    public DBType getDbType() {
        return dbType;
    }

    public String getConfigValue() {
        return configValue;
    }

    public String getRedisProperties() {
        return redisProperties;
    }
}
