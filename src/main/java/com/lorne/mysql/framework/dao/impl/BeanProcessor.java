package com.lorne.mysql.framework.dao.impl;

import java.beans.PropertyDescriptor;
import java.lang.reflect.Method;
import java.sql.*;
import java.util.Arrays;
import java.util.Map;

/**
 * Created by yuliang on 2015/8/5.
 */
public class BeanProcessor extends org.apache.commons.dbutils.BeanProcessor {

    public BeanProcessor() {
    }

    private Map<String, String> columnToPropertyOverrides;

    public BeanProcessor(Map<String, String> columnToPropertyOverrides) {
        super(columnToPropertyOverrides);
        this.columnToPropertyOverrides = columnToPropertyOverrides;
    }

    protected Object processColumn(ResultSet rs, int index, Class<?> propType)
            throws SQLException {
        if (!propType.isPrimitive() && rs.getObject(index) == null) {
            return null;
        }

        if (propType.equals(String.class)) {
            return rs.getString(index);

        } else if (
                propType.equals(Integer.TYPE) || propType.equals(Integer.class)) {
            return Integer.valueOf(rs.getInt(index));

        } else if (
                propType.equals(Boolean.TYPE) || propType.equals(Boolean.class)) {
            return Boolean.valueOf(rs.getBoolean(index));

        } else if (propType.equals(Long.TYPE) || propType.equals(Long.class)) {
            return Long.valueOf(rs.getLong(index));

        } else if (
                propType.equals(Double.TYPE) || propType.equals(Double.class)) {
            return Double.valueOf(rs.getDouble(index));

        } else if (
                propType.equals(Float.TYPE) || propType.equals(Float.class)) {
            return Float.valueOf(rs.getFloat(index));

        } else if (
                propType.equals(Short.TYPE) || propType.equals(Short.class)) {
            return Short.valueOf(rs.getShort(index));

        } else if (propType.equals(Byte.TYPE) || propType.equals(Byte.class)) {
            return Byte.valueOf(rs.getByte(index));

        } else if (propType.equals(Timestamp.class)) {
            return rs.getTimestamp(index);

        } else if (propType.equals(SQLXML.class)) {
            return rs.getSQLXML(index);
        } else if (propType.isEnum()) {
            try {
                Method getCode = propType.getMethod("getCode");
                if (getCode != null) {
                    Method method = propType.getMethod("valueOfCode", getCode.getReturnType());
                    Object obj = method.invoke(Enum.class, rs.getObject(index));
                    return obj;
                }
            } catch (Exception e) {
                return null;
            }
            return null;
        } else {
            return rs.getObject(index);
        }
    }


    protected int[] mapColumnsToProperties(ResultSetMetaData rsmd, PropertyDescriptor[] props) throws SQLException {
        int cols = rsmd.getColumnCount();
        int[] columnToProperty = new int[cols + 1];
        Arrays.fill(columnToProperty, -1);

        for (int col = 1; col <= cols; ++col) {
            String columnName = rsmd.getColumnLabel(col);
            if (null == columnName || 0 == columnName.length()) {
                columnName = rsmd.getColumnName(col);
            }

            String propertyName = (String) columnToPropertyOverrides.get(columnName.toUpperCase());
            if (propertyName == null) {
                propertyName = columnName;
            }

            for (int i = 0; i < props.length; ++i) {
                if (propertyName.equalsIgnoreCase(props[i].getName())) {
                    columnToProperty[col] = i;
                    break;
                }
            }
        }

        return columnToProperty;
    }
}
