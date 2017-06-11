package com.lorne.mysql.framework.utils;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.OracleHint;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.*;
import net.sf.jsqlparser.util.deparser.ExpressionDeParser;
import net.sf.jsqlparser.util.deparser.LimitDeparser;
import net.sf.jsqlparser.util.deparser.OrderByDeParser;

import java.util.Iterator;

/**
 * Created by yuliang on 2016/6/7.
 */

public class SelectCountParser extends net.sf.jsqlparser.util.deparser.SelectDeParser {

    private boolean isAddCount = false;

    public SelectCountParser(String sql) throws JSQLParserException {
        StringBuilder buffer = new StringBuilder();
        Select select = (Select) CCJSqlParserUtil.parse(sql);
        PlainSelect plainSelect = (PlainSelect) select.getSelectBody();
        ExpressionDeParser expressionDeParser = new ExpressionDeParser();
        super.setExpressionVisitor(expressionDeParser);
        super.setBuffer(buffer);
        expressionDeParser.setSelectVisitor(this);
        expressionDeParser.setBuffer(buffer);
        plainSelect.accept(this);
    }

    public String getCountSql() {
        return getBuffer().toString();
    }

    @Override
    public void visit(PlainSelect plainSelect) {
        if (plainSelect.isUseBrackets()) {
            getBuffer().append("(");
        }
        getBuffer().append("SELECT ");

        OracleHint hint = plainSelect.getOracleHint();
        if (hint != null) {
            getBuffer().append(hint).append(" ");
        }

        Skip skip = plainSelect.getSkip();
        if (skip != null) {
            getBuffer().append(skip).append(" ");
        }

        First first = plainSelect.getFirst();
        if (first != null) {
            getBuffer().append(first).append(" ");
        }

        if (plainSelect.getDistinct() != null) {
            getBuffer().append("DISTINCT ");
            if (plainSelect.getDistinct().getOnSelectItems() != null) {
                getBuffer().append("ON (");
                for (Iterator<SelectItem> iter = plainSelect.getDistinct().getOnSelectItems().iterator(); iter.hasNext(); ) {
                    SelectItem selectItem = iter.next();
                    selectItem.accept(this);
                    if (iter.hasNext()) {
                        getBuffer().append(", ");
                    }
                }
                getBuffer().append(") ");
            }

        }
        Top top = plainSelect.getTop();
        if (top != null) {
            getBuffer().append(top).append(" ");
        }

        for (Iterator<SelectItem> iter = plainSelect.getSelectItems().iterator(); iter.hasNext(); ) {
            SelectItem selectItem = iter.next();
            selectItem.accept(this);
            if (isAddCount)
                break;
            if (iter.hasNext()) {
                getBuffer().append(", ");
            }
        }

        if (plainSelect.getIntoTables() != null) {
            getBuffer().append(" INTO ");
            for (Iterator<Table> iter = plainSelect.getIntoTables().iterator(); iter.hasNext(); ) {
                visit(iter.next());
                if (iter.hasNext()) {
                    getBuffer().append(", ");
                }
            }
        }

        if (plainSelect.getFromItem() != null) {
            getBuffer().append(" FROM ");
            plainSelect.getFromItem().accept(this);
        }

        if (plainSelect.getJoins() != null) {
            for (Join join : plainSelect.getJoins()) {
                deparseJoin(join);
            }
        }

        if (plainSelect.getWhere() != null) {
            getBuffer().append(" WHERE ");
            plainSelect.getWhere().accept(getExpressionVisitor());
        }

        if (plainSelect.getOracleHierarchical() != null) {
            plainSelect.getOracleHierarchical().accept(getExpressionVisitor());
        }

        if (plainSelect.getGroupByColumnReferences() != null) {
            getBuffer().append(" GROUP BY ");
            for (Iterator<Expression> iter = plainSelect.getGroupByColumnReferences().iterator(); iter.hasNext(); ) {
                Expression columnReference = iter.next();
                columnReference.accept(getExpressionVisitor());
                if (iter.hasNext()) {
                    getBuffer().append(", ");
                }
            }
        }

        if (plainSelect.getHaving() != null) {
            getBuffer().append(" HAVING ");
            plainSelect.getHaving().accept(getExpressionVisitor());
        }

        if (plainSelect.getOrderByElements() != null) {
            new OrderByDeParser(getExpressionVisitor(), getBuffer()).deParse(plainSelect.isOracleSiblings(), plainSelect.getOrderByElements());
        }

        if (plainSelect.getLimit() != null) {
            new LimitDeparser(getBuffer()).deParse(plainSelect.getLimit());
        }
        if (plainSelect.getOffset() != null) {
            deparseOffset(plainSelect.getOffset());
        }
        if (plainSelect.getFetch() != null) {
            deparseFetch(plainSelect.getFetch());
        }
        if (plainSelect.isForUpdate()) {
            getBuffer().append(" FOR UPDATE");
            if (plainSelect.getForUpdateTable() != null) {
                getBuffer().append(" OF ").append(plainSelect.getForUpdateTable());
            }
        }
        if (plainSelect.isUseBrackets()) {
            getBuffer().append(")");
        }
    }

    private void addCount() {
        if (!isAddCount) {
            getBuffer().append("count(1)");
            isAddCount = !isAddCount;
        }
    }

    @Override
    public void visit(AllColumns allColumns) {
        //   super.visit(allColumns);
        if (!isAddCount)
            addCount();
        else
            super.visit(allColumns);
    }

    @Override
    public void visit(AllTableColumns allTableColumns) {
        // super.visit(allTableColumns);
        if (!isAddCount)
            addCount();
        else
            super.visit(allTableColumns);
    }

    @Override
    public void visit(SelectExpressionItem selectExpressionItem) {
        // super.visit(selectExpressionItem);
        if (!isAddCount)
            addCount();
        else
            super.visit(selectExpressionItem);
    }
}
