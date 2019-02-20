package com.xiwei.river;

import com.xiwei.river.core.ExeArgParser;
import com.xiwei.river.core.Processor;

import java.io.IOException;

public class Application {
    public static void main(String[] args) throws IOException {
        ExeArgParser exeArgParser = new ExeArgParser();
        exeArgParser.parse(args);

        Processor.exec(exeArgParser.getDbType(), exeArgParser.getConfigValue(), exeArgParser.getRedisProperties());
    }
}
