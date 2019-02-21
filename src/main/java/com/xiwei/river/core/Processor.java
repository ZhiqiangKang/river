package com.xiwei.river.core;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.xiwei.river.exception.ConfigException;
import com.xiwei.river.manager.JedisManager;
import com.xiwei.river.util.DateUtil;
import com.xiwei.river.util.ExceptionUtil;
import com.xiwei.river.vo.*;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.spark.SparkContext;
import org.apache.spark.SparkFiles;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructField;
import redis.clients.jedis.Jedis;
import scala.Tuple2;

import java.io.IOException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.spark.sql.types.DataTypes.*;

public class Processor {

    public static void exec(DBType dbType, String tableInfoFilePath, String redisPropertiesFilePath) throws IOException {
        TableInfo tableInfo = TableInfo.convertFromFile(tableInfoFilePath);

        Dataset<Row> srcRowDataset = readFromHive(tableInfo, redisPropertiesFilePath);

        Map<String, DataType> srcDataTypeMap = getDataTypeMap(srcRowDataset);

        if (DBType.HBASE.equals(dbType)){
            saveToHBase(srcRowDataset, srcDataTypeMap, tableInfo);
        } else if (DBType.REDIS.equals(dbType)){
            saveToRedis(srcRowDataset, srcDataTypeMap, tableInfo, redisPropertiesFilePath);
        }
    }

    /**
     * 从Hive中读取源表，并返回源数据结果集
     * @param tableInfo
     * @return 源数据结果集
     */
    private static Dataset<Row> readFromHive(TableInfo tableInfo, String redisPropertiesFilePath){
        String sql = tableInfo.getSql();
        String srcTableName = tableInfo.getSrcTable();

        SparkSession.Builder sparkSessionBuilder = SparkSession
                .builder()
                .appName(Thread.currentThread().getStackTrace()[1].getClassName())
                .enableHiveSupport();

        if ("true".equals(System.getProperty("debug"))){
            sparkSessionBuilder.master("local[*]");
        }

        SparkSession sparkSession = sparkSessionBuilder.getOrCreate();

        if (StringUtils.isNotBlank(redisPropertiesFilePath)){
            SparkContext sparkContext = sparkSession.sparkContext();
            sparkContext.addFile(redisPropertiesFilePath);
        }

        Dataset<Row> srcRowDataset = null;
        if (StringUtils.isNotBlank(sql)){
            srcRowDataset = sparkSession.sql(sql);
        } else if (StringUtils.isNotBlank(srcTableName)){
            srcRowDataset = sparkSession.table(srcTableName);
        } else {
            ExceptionUtil.throwException(ConfigException.class, "sql和srcTableName至少有一项不能为空");
        }

        return srcRowDataset;
    }

    /**
     * 从Dataset<Row>中解析，并返回一个以字段名为key，字段类型为value的map
     * @param rowDataset
     * @return
     */
    private static Map<String, DataType> getDataTypeMap(Dataset<Row> rowDataset){
        StructField[] srcStructFields = rowDataset.schema().fields();
        Map<String, DataType> dataTypeMap = Maps.newHashMap();
        for (StructField srcStructField : srcStructFields) {
            dataTypeMap.put(srcStructField.name(), srcStructField.dataType());
        }

        return dataTypeMap;
    }

    /**
     * 保存srcRowDataset至HBase
     * @param srcRowDataset
     * @param srcDataTypeMap
     * @param tableInfo
     * @throws IOException
     */
    private static void saveToHBase(Dataset<Row> srcRowDataset, final Map<String, DataType> srcDataTypeMap, TableInfo tableInfo) throws IOException {
        String destTableName = tableInfo.getDestTable();
        final String key = tableInfo.getKey();
        final List<FieldInfo> fieldInfoList = tableInfo.getFields();
        final Map<String, FieldInfo> fieldInfoMap = tableInfo.getFieldInfoMap();

        Configuration conf = HBaseConfiguration.create();
        conf.set(TableOutputFormat.OUTPUT_TABLE, destTableName);

        Job job = Job.getInstance(conf);
        job.setOutputKeyClass(ImmutableBytesWritable.class);
        job.setOutputValueClass(Put.class);
        job.setOutputFormatClass(TableOutputFormat.class);

        JavaPairRDD<ImmutableBytesWritable, Put> putJavaPairRDD = srcRowDataset.toJavaRDD().mapPartitionsToPair(new PairFlatMapFunction<Iterator<Row>, ImmutableBytesWritable, Put>() {
            @Override
            public Iterator<Tuple2<ImmutableBytesWritable, Put>> call(Iterator<Row> rowIterator) throws Exception {
                List<Tuple2<ImmutableBytesWritable, Put>> list = Lists.newArrayList();

                while (rowIterator.hasNext()){
                    Row row = rowIterator.next();
                    String keyFieldValue = getKeyFieldValue(row, key, srcDataTypeMap, fieldInfoMap);
                    Put put = new Put(keyFieldValue.getBytes());

                    for (FieldInfo fieldInfo : fieldInfoList) {
                        String fieldName = fieldInfo.getName();
                        String newFieldName = fieldInfo.getNewName();
                        // 若newName未配置，则取name作为newName
                        if (StringUtils.isBlank(newFieldName)) {
                            newFieldName = fieldName;
                        }

                        String family = Constant.DEFAULT_HBASE_FAMILY;
                        /*  列族名称默认为"cols"。
                            若newName中包含":"，则以":"切分，取前半部分作为列族名称，后半部分作为列名
                         */
                        if (newFieldName.contains(":")) {
                            String[] familyAndQualifier = StringUtils.split(":");
                            family = familyAndQualifier[0];
                            newFieldName = familyAndQualifier[1];
                        }

                        // 源表中字段数据类型
                        DataType srcDataType = srcDataTypeMap.get(fieldName);
                        Pair<String, byte[]> pair = convertValueToStringAndByteArray(srcDataType, row, fieldInfo);
                        byte[] bytes = pair.getValue();
                        put.addColumn(family.getBytes(), newFieldName.getBytes(), bytes);
                    }
                    list.add(new Tuple2<>(new ImmutableBytesWritable(), put));
                }

                return list.iterator();
            }
        });

        putJavaPairRDD.saveAsNewAPIHadoopDataset(job.getConfiguration());
    }

    /**
     * 保存srcRowDataset至Redis
     * @param srcRowDataset
     * @param srcDataTypeMap
     * @param tableInfo
     * @throws IOException
     */
    private static void saveToRedis(Dataset<Row> srcRowDataset, final Map<String, DataType> srcDataTypeMap, TableInfo tableInfo, final String redisPropertiesFilePath) throws IOException {
        final String destTableName = tableInfo.getDestTable();
        final String key = tableInfo.getKey();
        final List<FieldInfo> fieldInfoList = tableInfo.getFields();
        final Map<String, FieldInfo> fieldInfoMap = tableInfo.getFieldInfoMap();

        srcRowDataset.toJavaRDD().foreachPartition(new VoidFunction<Iterator<Row>>() {
            @Override
            public void call(Iterator<Row> rowIterator) throws Exception {
                Jedis jedis = JedisManager.getJedis(SparkFiles.get(redisPropertiesFilePath));

                while (rowIterator.hasNext()) {
                    Row row = rowIterator.next();
                    String keyFieldValue = getKeyFieldValue(row, key, srcDataTypeMap, fieldInfoMap);
                    keyFieldValue = destTableName + ":" + keyFieldValue;

                    Map<byte[], byte[]> map = Maps.newHashMap();
                    for (FieldInfo fieldInfo : fieldInfoList) {
                        String fieldName = fieldInfo.getName();
                        String newFieldName = fieldInfo.getNewName();
                        // 若newName未配置，则取name作为newName
                        if (StringUtils.isBlank(newFieldName)) {
                            newFieldName = fieldName;
                        }

                        // 源表中字段数据类型
                        DataType srcDataType = srcDataTypeMap.get(fieldName);
                        Pair<String, byte[]> pair = convertValueToStringAndByteArray(srcDataType, row, fieldInfo);
                        byte[] bytes = pair.getValue();
                        map.put(newFieldName.getBytes(), bytes);
                    }
                    jedis.hmset(keyFieldValue.getBytes(), map);
                }
            }
        });
    }

    /**
     * 根据配置的key获取值
     * @param row
     * @param key
     * @return
     */
    private static String getKeyFieldValue(Row row, String key, Map<String, DataType> srcDataTypeMap, Map<String, FieldInfo> fieldInfoMap){
        // 正则匹配${}
        Pattern pattern = Pattern.compile("\\$\\{\\w+\\}");
        Matcher matcher = pattern.matcher(key);

        Set<String> varSet = Sets.newHashSet();
        while (matcher.find()){
            String e = matcher.group();

            // 取出需要替换的变量
            String var = e.substring(2, e.length() -1);
            varSet.add(var);
        }

        for (String var : varSet) {
            Pair<String, byte[]> pair = convertValueToStringAndByteArray(srcDataTypeMap.get(var), row, fieldInfoMap.get(var));

            String value = pair.getKey();

            key = key.replace("${" + var + "}", value);
        }

        return key;
    }

    /**
     * 根据配置的字段数据类型及格式信息，将字段值转换为<String类型, 字节数组>，并返回
     * @param dataType
     * @param row
     * @param fieldInfo
     * @return
     */
    private static Pair<String, byte[]> convertValueToStringAndByteArray(DataType dataType, Row row, FieldInfo fieldInfo){
        String fieldName = fieldInfo.getName();
        Object o = row.getAs(fieldName);
        if (o == null) return null;

        DataTypeInfo newDataType = fieldInfo.getNewDataTypeInfo();
        byte[] byteArrayValue = null;
        String strValue = null;
        if (ByteType.sameType(dataType)){
            byte fieldValue = row.getAs(fieldName);
            strValue = String.valueOf(fieldValue);
            switch (newDataType){
                case BOOLEAN:
                    byteArrayValue = Bytes.toBytes(Boolean.valueOf(strValue));
                    break;
                case FLOAT:
                    byteArrayValue = Bytes.toBytes(Float.valueOf(fieldValue));
                    break;
                case DOUBLE:
                    byteArrayValue = Bytes.toBytes(Double.valueOf(fieldValue));
                    break;
                case STRING:
                    byteArrayValue = Bytes.toBytes(strValue);
                    break;
                default:
                    byteArrayValue = Bytes.toBytes(fieldValue);
            }
        } else if (ShortType.sameType(dataType)){
            short fieldValue = row.getAs(fieldName);
            strValue = String.valueOf(fieldValue);
            switch (newDataType){
                case BYTE:
                    byteArrayValue = Bytes.toBytes(Byte.valueOf(strValue));
                    break;
                case BOOLEAN:
                    byteArrayValue = Bytes.toBytes(Boolean.valueOf(strValue));
                    break;
                case FLOAT:
                    byteArrayValue = Bytes.toBytes(Float.valueOf(fieldValue));
                    break;
                case DOUBLE:
                    byteArrayValue = Bytes.toBytes(Double.valueOf(fieldValue));
                    break;
                case STRING:
                    byteArrayValue = Bytes.toBytes(strValue);
                    break;
                default:
                    byteArrayValue = Bytes.toBytes(fieldValue);
            }
        } else if (IntegerType.sameType(dataType)){
            int fieldValue = row.getAs(fieldName);
            strValue = String.valueOf(fieldValue);
            switch (newDataType){
                case BYTE:
                    byteArrayValue = Bytes.toBytes(Byte.valueOf(strValue));
                    break;
                case SHORT:
                    byteArrayValue = Bytes.toBytes(Short.valueOf(strValue));
                    break;
                case BOOLEAN:
                    byteArrayValue = Bytes.toBytes(Boolean.valueOf(strValue));
                    break;
                case FLOAT:
                    byteArrayValue = Bytes.toBytes(Float.valueOf(fieldValue));
                    break;
                case DOUBLE:
                    byteArrayValue = Bytes.toBytes(Double.valueOf(fieldValue));
                    break;
                case STRING:
                    byteArrayValue = Bytes.toBytes(strValue);
                    break;
                default:
                    byteArrayValue = Bytes.toBytes(fieldValue);
            }
        } else if (LongType.sameType(dataType)){
            long fieldValue = row.getAs(fieldName);
            strValue = String.valueOf(fieldValue);
            switch (newDataType){
                case BYTE:
                    byteArrayValue = Bytes.toBytes(Byte.valueOf(strValue));
                    break;
                case SHORT:
                    byteArrayValue = Bytes.toBytes(Short.valueOf(strValue));
                    break;
                case INTEGER:
                    byteArrayValue = Bytes.toBytes(Integer.valueOf(strValue));
                    break;
                case BOOLEAN:
                    byteArrayValue = Bytes.toBytes(Boolean.valueOf(strValue));
                    break;
                case FLOAT:
                    byteArrayValue = Bytes.toBytes(Float.valueOf(fieldValue));
                    break;
                case DOUBLE:
                    byteArrayValue = Bytes.toBytes(Double.valueOf(fieldValue));
                    break;
                case STRING:
                    byteArrayValue = Bytes.toBytes(strValue);
                    break;
                default:
                    byteArrayValue = Bytes.toBytes(fieldValue);
            }
        } else if (FloatType.sameType(dataType)){
            float fieldValue = row.getAs(fieldName);
            strValue = String.valueOf(fieldValue);
            switch (newDataType){
                case BYTE:
                    byteArrayValue = Bytes.toBytes(Byte.valueOf(strValue));
                    break;
                case SHORT:
                    byteArrayValue = Bytes.toBytes(Short.valueOf(strValue));
                    break;
                case INTEGER:
                    byteArrayValue = Bytes.toBytes(Integer.valueOf(strValue));
                    break;
                case LONG:
                    byteArrayValue = Bytes.toBytes(Long.valueOf(strValue));
                    break;
                case BOOLEAN:
                    byteArrayValue = Bytes.toBytes(Boolean.valueOf(strValue));
                    break;
                case STRING:
                    byteArrayValue = Bytes.toBytes(strValue);
                    break;
                default:
                    byteArrayValue = Bytes.toBytes(fieldValue);
            }
        } else if (DoubleType.sameType(dataType)){
            double fieldValue = row.getAs(fieldName);
            strValue = String.valueOf(fieldValue);
            switch (newDataType){
                case BYTE:
                    byteArrayValue = Bytes.toBytes(Byte.valueOf(strValue));
                    break;
                case SHORT:
                    byteArrayValue = Bytes.toBytes(Short.valueOf(strValue));
                    break;
                case INTEGER:
                    byteArrayValue = Bytes.toBytes(Integer.valueOf(strValue));
                    break;
                case LONG:
                    byteArrayValue = Bytes.toBytes(Long.valueOf(strValue));
                    break;
                case BOOLEAN:
                    byteArrayValue = Bytes.toBytes(Boolean.valueOf(strValue));
                    break;
                case FLOAT:
                    byteArrayValue = Bytes.toBytes(Float.valueOf(strValue));
                    break;
                case STRING:
                    byteArrayValue = Bytes.toBytes(strValue);
                    break;
                default:
                    byteArrayValue = Bytes.toBytes(fieldValue);
            }
        } else if (BooleanType.sameType(dataType)){
            boolean fieldValue = row.getAs(fieldName);
            strValue = String.valueOf(fieldValue);
            switch (newDataType){
                case BYTE:
                    byteArrayValue = Bytes.toBytes(Byte.valueOf(strValue));
                    break;
                case SHORT:
                    byteArrayValue = Bytes.toBytes(Short.valueOf(strValue));
                    break;
                case INTEGER:
                    byteArrayValue = Bytes.toBytes(Integer.valueOf(strValue));
                    break;
                case LONG:
                    byteArrayValue = Bytes.toBytes(Long.valueOf(strValue));
                    break;
                case FLOAT:
                    byteArrayValue = Bytes.toBytes(Float.valueOf(strValue));
                    break;
                case DOUBLE:
                    byteArrayValue = Bytes.toBytes(Double.valueOf(strValue));
                    break;
                case STRING:
                    byteArrayValue = Bytes.toBytes(strValue);
                    break;
                default:
                    byteArrayValue = Bytes.toBytes(fieldValue);
            }
        } else if (StringType.sameType(dataType)){
            strValue = row.getAs(fieldName);
            switch (newDataType){
                case BYTE:
                    byteArrayValue = Bytes.toBytes(Byte.valueOf(strValue));
                    break;
                case SHORT:
                    byteArrayValue = Bytes.toBytes(Short.valueOf(strValue));
                    break;
                case INTEGER:
                    byteArrayValue = Bytes.toBytes(Integer.valueOf(strValue));
                    break;
                case LONG:
                    byteArrayValue = Bytes.toBytes(Long.valueOf(strValue));
                    break;
                case BOOLEAN:
                    byteArrayValue = Bytes.toBytes(Boolean.valueOf(strValue));
                    break;
                case FLOAT:
                    byteArrayValue = Bytes.toBytes(Float.valueOf(strValue));
                    break;
                case DOUBLE:
                    byteArrayValue = Bytes.toBytes(Double.valueOf(strValue));
                    break;
                default:
                    String datePattern = fieldInfo.getDatePattern();
                    String newDatePattern = fieldInfo.getNewDatePattern();
                    if (StringUtils.isNotBlank(newDatePattern)){
                        if (StringUtils.isBlank(datePattern)){
                            datePattern = Constant.DEFAULT_DATE_PATTERN;
                        }
                        Date date = DateUtil.parse(strValue, datePattern);
                        strValue = DateUtil.format(date, newDatePattern);
                    }

                    byteArrayValue = Bytes.toBytes(strValue);
            }
        } else if (DateType.sameType(dataType)){
            Date fieldValue = row.getAs(fieldName);
            String newDatePattern = fieldInfo.getNewDatePattern();
            if (StringUtils.isBlank(newDatePattern)){
                newDatePattern = Constant.DEFAULT_DATE_PATTERN;
            }
            strValue = DateUtil.format(fieldValue, newDatePattern);
            byteArrayValue = Bytes.toBytes(strValue);
        }

        Pair<String, byte[]> pair = Pair.of(strValue, byteArrayValue);
        return pair;
    }
}
