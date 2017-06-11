package com.lorne.mysql.framework.utils;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.select.Limit;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;

/**
 * Created by yuliang on 2016/6/7.
 */
public class SelectLimitUtils {

    public static String getLimitSql(String sql, int nowPage, int pageSize) throws JSQLParserException {
        Select select = (Select) CCJSqlParserUtil.parse(sql);
        PlainSelect plainSelect = (PlainSelect) select.getSelectBody();
        Limit limit = new Limit();
        limit.setOffset((nowPage - 1) * pageSize);
        limit.setRowCount(pageSize);
        plainSelect.setLimit(limit);
        return select.toString();
    }
}
