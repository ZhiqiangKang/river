package com.xiwei.river.util;

import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;

import java.util.Date;

public class DateUtil {

    public static String format(Date date, String pattern) {
        return new DateTime(date).toString(pattern);
    }

    public static Date parse(String dateString, String pattern) {
        return DateTime.parse(dateString, DateTimeFormat.forPattern(pattern)).toDate();
    }

    public static void main(String[] args) {

    }
}
