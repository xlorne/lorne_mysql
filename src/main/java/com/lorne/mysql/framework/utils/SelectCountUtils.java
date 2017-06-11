package com.lorne.mysql.framework.utils;

import net.sf.jsqlparser.JSQLParserException;

/**
 * Created by yuliang on 2016/6/7.
 */
public class SelectCountUtils {

    public static String getCountSql(String sql) throws JSQLParserException {
        SelectCountParser parser = new SelectCountParser(sql);
        return parser.getCountSql();
    }
}
