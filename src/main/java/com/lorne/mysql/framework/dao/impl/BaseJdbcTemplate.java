package com.lorne.mysql.framework.dao.impl;


import com.lorne.core.framework.annotation.db.*;
import com.lorne.core.framework.model.BaseEntity;
import com.lorne.core.framework.model.Page;
import com.lorne.mysql.framework.utils.SelectCountUtils;
import com.lorne.mysql.framework.utils.SelectLimitUtils;
import net.sf.jsqlparser.JSQLParserException;
import org.apache.commons.beanutils.PropertyUtils;
import org.apache.commons.dbutils.BasicRowProcessor;
import org.apache.commons.dbutils.RowProcessor;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.PreparedStatementCreator;
import org.springframework.jdbc.core.ResultSetExtractor;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.KeyHolder;

import java.beans.BeanInfo;
import java.beans.IntrospectionException;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.io.Serializable;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.sql.*;
import java.util.*;

public class BaseJdbcTemplate<T extends BaseEntity> {

    private RowProcessor defaultConvert = new BasicRowProcessor();

    /**
     * 当前Table类对象
     */
    protected Class<?> clazz;

    /**
     * 当前表名称
     */
    protected String tableName;


    protected String className;

    protected String idName;

    protected String generatorProperty;

    private Map<String, String> columnToPropertyOverrides;

    private Map<String, String> propertyToColumnOverrides;

    private String insertSql;

    private String updateSql;

    private JdbcTemplate jdbcTemplate;

    public JdbcTemplate getJdbcTemplate() {
        return jdbcTemplate;
    }

    public void setJdbcTemplate(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public Class<?> getClazz() {
        return clazz;
    }


    protected String loadTableName(String tableName) {
        return tableName;
    }


    public BaseJdbcTemplate() {

        this.clazz = (Class<?>) ((ParameterizedType) getClass()
                .getGenericSuperclass()).getActualTypeArguments()[0];


        className = this.clazz.getSimpleName();

        Table table = clazz.getAnnotation(Table.class);
        if (table == null) {
            tableName = clazz.getSimpleName();
        } else {
            tableName = table.name();
        }
        tableName = loadTableName(tableName);

        if (columnToPropertyOverrides == null)
            columnToPropertyOverrides = getColumnToPropertyOverrides();
        if (propertyToColumnOverrides == null)
            propertyToColumnOverrides = getPropertyToColumnOverrides();

        insertSql = getInsertSql();
        updateSql = getUpdateSql();

    }

    private String getInsertSql() {
        if (propertyToColumnOverrides == null || propertyToColumnOverrides.size() == 0) {
            return null;
        }
        String sql = "insert into  {table} ({columns}) values ({values})";
        String columns = "";
        String values = "";
        Set<String> sets = propertyToColumnOverrides.keySet();
        boolean isAdd = false;
        for (String propertyName : sets) {
            if (generatorProperty != null) {
                if (generatorProperty.equals(propertyName))
                    continue;
            }
            String columnName = propertyToColumnOverrides.get(propertyName);
            columns += columnName + ",";
            values += "?,";
            isAdd = true;
        }
        if (isAdd) {
            columns = columns.substring(0, columns.length() - 1);
            values = values.substring(0, values.length() - 1);
        }
        sql = sql.replace("{columns}", columns);
        sql = sql.replace("{values}", values);
        return sql;
    }

    private String getUpdateSql() {
        if (propertyToColumnOverrides == null || propertyToColumnOverrides.size() == 0) {
            return null;
        }
        String sql = "update {table} set {set} where {id} = ? ";
        String set = "";
        Set<String> sets = propertyToColumnOverrides.keySet();
        boolean isAdd = false;
        for (String propertyName : sets) {
            if (generatorProperty != null) {
                if (generatorProperty.equals(propertyName))
                    continue;
            }
            String columnName = propertyToColumnOverrides.get(propertyName);
            set += columnName + "= ? ,";
            isAdd = true;
        }
        if (isAdd) {
            set = set.substring(0, set.length() - 1);
        }
        sql = sql.replace("{set}", set);
        sql = sql.replace("{id}", idName);
        return sql;
    }


    private Map<String, String> getColumnToPropertyOverrides() {
        Map<String, String> maps = new HashMap<String, String>();
        PropertyDescriptor[] propertyDescriptors = new PropertyDescriptor[0];
        try {
            propertyDescriptors = propertyDescriptors(clazz);
            for (PropertyDescriptor propertyDescriptor : propertyDescriptors) {
                Method gmethod = propertyDescriptor.getReadMethod();
                if (null != gmethod) {
                    /**
                     * 反射字段名称
                     */
                    Column column = gmethod.getAnnotation(Column.class);
                    String columnName = "";
                    if (column == null) {
                        columnName = propertyToColumn(propertyDescriptor.getName());
                    } else {
                        columnName = column.name();
                    }
                    maps.put(columnName.toUpperCase(), propertyDescriptor.getName());
                }
            }
            return maps;
        } catch (SQLException e) {
            return null;
        }
    }

    private Map<String, String> getPropertyToColumnOverrides() {
        Map<String, String> maps = new HashMap<String, String>();
        PropertyDescriptor[] propertyDescriptors = new PropertyDescriptor[0];
        try {
            propertyDescriptors = propertyDescriptors(clazz);
            for (PropertyDescriptor propertyDescriptor : propertyDescriptors) {
                Method gmethod = propertyDescriptor.getReadMethod();
                Method smethod = propertyDescriptor.getWriteMethod();
                if (null != gmethod && smethod != null) {

                    /**
                     * 忽略字段
                     */
                    Transient aTransient = gmethod.getAnnotation(Transient.class);
                    if (aTransient != null)
                        continue;

                    /**
                     * 反射字段名称
                     */
                    Column column = gmethod.getAnnotation(Column.class);

                    Generator generator = gmethod.getAnnotation(Generator.class);

                    if (generator != null) {
                        generatorProperty = propertyDescriptor.getName();
                    }
                    String columnName;
                    if (column == null) {
                        columnName = propertyToColumn(propertyDescriptor.getName());
                    } else {
                        columnName = column.name();
                    }
                    Id id = gmethod.getAnnotation(Id.class);
                    if (id != null) {
                        idName = columnName;
                    }
                    columnName = columnName.toUpperCase();
                    maps.put(propertyDescriptor.getName(), columnName);
                }
            }
            if (idName == null || "".equals(idName)) {
                throw new RuntimeException(className + " id not exist!");
            }
            return maps;
        } catch (SQLException e) {
            return null;
        }
    }


    private String propertyToColumn(String propertyName) {
        char[] chs = propertyName.toCharArray();
        StringBuffer sb = new StringBuffer();
        for (int i = 0; i < chs.length; i++) {
            char c = chs[i];
            int index = c;
            if (index < 97) {
                sb.append("_");
                sb.append((char) (c + 32));
            } else {
                sb.append(c);
            }
        }
        return sb.toString();
    }


    private PropertyDescriptor[] propertyDescriptors(Class<?> c) throws SQLException {
        BeanInfo beanInfo = null;

        try {
            beanInfo = Introspector.getBeanInfo(c);
        } catch (IntrospectionException var4) {
            throw new SQLException("Bean introspection failed: " + var4.getMessage());
        }

        return beanInfo.getPropertyDescriptors();
    }


    public Map<String, Object> toMap(ResultSet rs) throws SQLException {
        Map<String, Object> result = new HashMap<String, Object>();
        ResultSetMetaData rsmd = rs.getMetaData();
        int cols = rsmd.getColumnCount();

        for (int i = 1; i <= cols; i++) {
            String columnName = rsmd.getColumnLabel(i);
            if (null == columnName || 0 == columnName.length()) {
                columnName = rsmd.getColumnName(i);
            }
            String propertyName = columnToPropertyOverrides.get(columnName.toUpperCase());
            if (propertyName != null) {
                result.put(propertyName, rs.getObject(i));
            } else {
                result.put(columnName, rs.getObject(i));
            }
        }

        return result;
    }

    public <T> T toBean(ResultSet rs, Map<String, String> columnToPropertyOverrides) throws SQLException {
        RowProcessor convert = null;
        if (columnToPropertyOverrides != null) {
            convert = new BasicRowProcessor(new BeanProcessor(columnToPropertyOverrides));
        } else {
            convert = defaultConvert;
        }
        return (T) convert.toBean(rs, clazz);
    }

    public String initSql(String sql) {
        while (sql.contains("  "))
            sql = sql.replace("  ", " ");
        sql = sql.replace("{table}", tableName);
        sql = sql.replace("{id}", idName);
        if (propertyToColumnOverrides != null)
            for (String key : propertyToColumnOverrides.keySet()) {
                sql = sql.replace("{" + key + "}", propertyToColumnOverrides.get(key));
            }
        return sql;
    }

    public <T> T queryForBean(String sql) {
        return getJdbcTemplate().query(initSql(sql), new ResultSetExtractor<T>() {
            public T extractData(ResultSet rs) throws SQLException, DataAccessException {
                return rs.next() ? (T) toBean(rs, columnToPropertyOverrides) : null;
            }
        });
    }

    public Map<String, Object> queryForMap(String sql) {
        return getJdbcTemplate().query(initSql(sql), new ResultSetExtractor<Map<String, Object>>() {
            public Map<String, Object> extractData(ResultSet rs) throws SQLException, DataAccessException {
                return rs.next() ? toMap(rs) : null;
            }
        });
    }

    public Map<String, Object> queryForMap(String sql, Object... args) {
        return getJdbcTemplate().query(initSql(sql), args, new ResultSetExtractor<Map<String, Object>>() {
            public Map<String, Object> extractData(ResultSet rs) throws SQLException, DataAccessException {
                return rs.next() ? toMap(rs) : null;
            }
        });
    }

    public List<Map<String, Object>> queryForMapList(String sql) {
        return getJdbcTemplate().query(initSql(sql), new RowMapper<Map<String, Object>>() {
            public Map<String, Object> mapRow(ResultSet rs, int i) throws SQLException {
                return toMap(rs);
            }
        });
    }

    public List<Map<String, Object>> queryForMapList(String sql, Object... args) {
        return getJdbcTemplate().query(initSql(sql), args, new RowMapper<Map<String, Object>>() {
            public Map<String, Object> mapRow(ResultSet rs, int i) throws SQLException {
                return toMap(rs);
            }
        });
    }

    public <T> T queryForBean(String sql, final Map<String, String> columnToPropertyOverrides, Object... args) {
        return getJdbcTemplate().query(initSql(sql), args, new ResultSetExtractor<T>() {
            public T extractData(ResultSet rs) throws SQLException, DataAccessException {
                return rs.next() ? (T) toBean(rs, columnToPropertyOverrides) : null;
            }
        });
    }

    public <T> List<T> queryForBeanList(String sql, final Map<String, String> columnToPropertyOverrides, Object... args) {
        return getJdbcTemplate().query(initSql(sql), args, new RowMapper<T>() {
            public T mapRow(ResultSet rs, int i) throws SQLException {
                return (T) toBean(rs, columnToPropertyOverrides);
            }
        });
    }

    public <T> T queryForBean(String sql, Object... args) {
        return getJdbcTemplate().query(initSql(sql), args, new ResultSetExtractor<T>() {
            public T extractData(ResultSet rs) throws SQLException, DataAccessException {
                return rs.next() ? (T) toBean(rs, columnToPropertyOverrides) : null;
            }
        });


    }

    public <T> List<T> queryForBeanList(String sql) {
        return getJdbcTemplate().query(initSql(sql), new RowMapper<T>() {
            public T mapRow(ResultSet rs, int i) throws SQLException {
                return (T) toBean(rs, columnToPropertyOverrides);
            }
        });
    }

    public <T> List<T> queryForBeanList(String sql, Object... args) {
        return getJdbcTemplate().query(initSql(sql), args, new RowMapper<T>() {
            public T mapRow(ResultSet rs, int i) throws SQLException {
                return (T) toBean(rs, columnToPropertyOverrides);
            }
        });
    }


    private Object getValueByPropertyName(T t, String propertyName) {
        //通过bean 的 property字段的get方法获取value
        try {
            PropertyDescriptor propertyDescriptor = PropertyUtils.getPropertyDescriptor(t, propertyName);
            Method method = propertyDescriptor.getReadMethod();
            Object val = method.invoke(t);
            return val;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    public Page<Map<String, Object>> pageForMapList(String sql, int nowPage, int pageSize) {
        return pageForMapList(sql, nowPage, pageSize, null);
    }

    public Page<Map<String, Object>> pageForMapList(String sql, int nowPage, int pageSize, Object... args) {
        sql = initSql(sql);
        String countSql = null;
        String limitSql = null;
        try {
            countSql = SelectCountUtils.getCountSql(sql);
            limitSql = SelectLimitUtils.getLimitSql(sql, nowPage, pageSize);
        } catch (JSQLParserException e) {
            System.out.println(sql + ",格式不正确,请检查后再试.");
            return null;
        }

        Page<Map<String, Object>> page = new Page<Map<String, Object>>();
        List<Object> objs = new ArrayList<Object>();
        if (args != null && args.length > 0) {
            for (Object obj : args) {
                objs.add(obj);
            }
        }
        int count = queryForInt(countSql, objs.toArray());
        page.setTotal(count);

        List<Map<String, Object>> list = getJdbcTemplate().query(limitSql, objs.toArray(), new RowMapper<Map<String, Object>>() {
            public Map<String, Object> mapRow(ResultSet rs, int i) throws SQLException {
                return toMap(rs);
            }
        });
        page.setRows(list);
        page.setNowPage(nowPage);
        page.setPageSize(pageSize);

        int pageNumber = 0;
        if (count > 0) {
            if (count % pageSize == 0) {
                pageNumber = count / pageSize;
            } else {
                pageNumber = count / pageSize + 1;
            }
        }
        page.setPageNumber(pageNumber);
        return page;
    }

    public <T> Page<T> pageForBeanList(String sql, int nowPage, int pageSize) {
        return pageForBeanList(sql, nowPage, pageSize, null);
    }

    public <T> Page<T> pageForBeanList(String sql, int nowPage, int pageSize, final Map<String, String> columnToPropertyOverrides, Object... args) {
        sql = initSql(sql);
        String countSql = null;
        String limitSql = null;
        try {
            countSql = SelectCountUtils.getCountSql(sql);
            limitSql = SelectLimitUtils.getLimitSql(sql, nowPage, pageSize);
        } catch (JSQLParserException e) {
            System.out.println(sql + ",格式不正确,请检查后再试.");
            return null;
        }

        Page<T> page = new Page<T>();
        List<Object> objs = new ArrayList<Object>();
        if (args != null && args.length > 0) {
            for (Object obj : args) {
                objs.add(obj);
            }
        }
        int count = queryForInt(countSql, objs.toArray());
        page.setTotal(count);

        List<T> list = getJdbcTemplate().query(limitSql, objs.toArray(), new RowMapper<T>() {
            public T mapRow(ResultSet rs, int i) throws SQLException {
                return (T) toBean(rs, columnToPropertyOverrides);
            }
        });
        page.setRows(list);
        page.setNowPage(nowPage);
        page.setPageSize(pageSize);

        int pageNumber = 0;
        if (count > 0) {
            if (count % pageSize == 0) {
                pageNumber = count / pageSize;
            } else {
                pageNumber = count / pageSize + 1;
            }
        }
        page.setPageNumber(pageNumber);
        return page;
    }

    public <T> Page<T> pageForBeanList(String sql, int nowPage, int pageSize, Object... args) {
        return pageForBeanList(sql, nowPage, pageSize, columnToPropertyOverrides, args);
    }

    public int queryForInt(String sql, Object... args) {
        if (args != null && args.length > 0)
            return getJdbcTemplate().query(initSql(sql), args, new ResultSetExtractor<Integer>() {
                public Integer extractData(ResultSet rs) throws SQLException, DataAccessException {
                    return rs.next() ? rs.getInt(1) : 0;
                }
            });
        else
            return getJdbcTemplate().query(initSql(sql), new ResultSetExtractor<Integer>() {
                public Integer extractData(ResultSet rs) throws SQLException, DataAccessException {
                    return rs.next() ? rs.getInt(1) : 0;
                }
            });

    }

    public int queryForInt(String sql) {
        return queryForInt(sql, null);
    }

    public <T> Page<T> pageAll(int nowPage, int pageSize) {
        String sql = "select * from " + tableName;
        return pageForBeanList(sql, nowPage, pageSize);
    }


    public List<Map<String, Object>> listForMapList(String sql, int nowPage, int pageSize) {
        return listForMapList(sql, nowPage, pageSize, null);
    }

    public List<Map<String, Object>> listForMapList(String sql, int nowPage, int pageSize, Object... args) {
        sql = initSql(sql);
        String limitSql = null;
        try {
            limitSql = SelectLimitUtils.getLimitSql(sql, nowPage, pageSize);
        } catch (JSQLParserException e) {
            System.out.println(sql + ",格式不正确,请检查后再试.");
            return null;
        }

        Page<Map<String, Object>> page = new Page<Map<String, Object>>();
        List<Object> objs = new ArrayList<Object>();
        if (args != null && args.length > 0) {
            for (Object obj : args) {
                objs.add(obj);
            }
        }

        return getJdbcTemplate().query(limitSql, objs.toArray(), new RowMapper<Map<String, Object>>() {
            public Map<String, Object> mapRow(ResultSet rs, int i) throws SQLException {
                return toMap(rs);
            }
        });
    }

    public <T> List<T> listForBeanList(String sql, int nowPage, int pageSize) {
        return listForBeanList(sql, nowPage, pageSize, columnToPropertyOverrides, null);
    }

    public <T> List<T> listForBeanList(String sql, int nowPage, int pageSize, final Map<String, String> columnToPropertyOverrides, Object... args) {
        sql = initSql(sql);
        String limitSql = null;
        try {
            limitSql = SelectLimitUtils.getLimitSql(sql, nowPage, pageSize);
        } catch (JSQLParserException e) {
            System.out.println(sql + ",格式不正确,请检查后再试.");
            return null;
        }
        List<Object> objs = new ArrayList<Object>();
        if (args != null && args.length > 0) {
            for (Object obj : args) {
                objs.add(obj);
            }
        }
        return getJdbcTemplate().query(limitSql, objs.toArray(), new RowMapper<T>() {
            public T mapRow(ResultSet rs, int i) throws SQLException {
                return (T) toBean(rs, columnToPropertyOverrides);
            }
        });

    }

    public <T> List<T> listForBeanList(String sql, int nowPage, int pageSize, Object... args) {
        return listForBeanList(sql, nowPage, pageSize, columnToPropertyOverrides, args);
    }

    public <T> List<T> listAll(int nowPage, int pageSize) {
        String sql = "select * from {table}";
        return listForBeanList(sql, nowPage, pageSize);
    }


    /***********************************
     * update
     ********************************/
    public int update(String sql, Object... args) {
        sql = initSql(sql);
        int f = getJdbcTemplate().update(sql, args);
        return f;
    }


    public int update(T t) {
        if (propertyToColumnOverrides == null || propertyToColumnOverrides.size() == 0) {
            return 0;
        }
        List<Object> objects = new ArrayList<Object>();
        Set<String> sets = propertyToColumnOverrides.keySet();
        Object idVal = null;
        for (String propertyName : sets) {
            Object val = getValueByPropertyName(t, propertyName);
            //如果是枚举类型，获取code的值
            if (val != null && val.getClass().isEnum()) {
                try {
                    Method method = val.getClass().getMethod("getCode");
                    try {
                        val = method.invoke(val);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                } catch (NoSuchMethodException e) {
                    e.printStackTrace();
                }
            }
            if (generatorProperty != null) {
                if (generatorProperty.equals(propertyName)) {
                    idVal = val;
                    continue;
                }

            }
            objects.add(val);
        }
        objects.add(idVal);
        //updateSql 没有替换
        return update(updateSql, objects.toArray());
    }

    public Long insertAndGetKey(final String sql, final Object... args) {
        final String mSql = initSql(sql);
        KeyHolder keyHolder = new GeneratedKeyHolder();
        getJdbcTemplate().update(new PreparedStatementCreator() {
            public PreparedStatement createPreparedStatement(Connection connection) throws SQLException {
                PreparedStatement ps = connection.prepareStatement(mSql, Statement.RETURN_GENERATED_KEYS);
                if (args != null && args.length > 0) {
                    for (int index = 0; index < args.length; index++) {
                        ps.setObject((index + 1), args[index]);
                    }
                }
                return ps;
            }
        }, keyHolder);
        Long generatedId = keyHolder.getKey().longValue();
        return generatedId;
    }

    public int[] batchUpdate(String sql, List<Object[]> objects) {
        sql = initSql(sql);
        return getJdbcTemplate().batchUpdate(sql, objects);
    }


    private void pushUUIDtoArray(List<String> uuids, Object id) {
        if (id != null) {
            if (id instanceof Serializable)
                uuids.add(String.valueOf(id));
        }
    }

    public long save(T t) {
        if (propertyToColumnOverrides == null || propertyToColumnOverrides.size() == 0) {
            return 0l;
        }
        List<Object> objects = new ArrayList<Object>();
        Set<String> sets = propertyToColumnOverrides.keySet();
        for (String propertyName : sets) {
            if (generatorProperty != null) {
                if (generatorProperty.equals(propertyName))
                    continue;
            }
            Object val = getValueByPropertyName(t, propertyName);
            //如果是枚举类型，获取code的值
            if (val != null && val.getClass().isEnum()) {
                try {
                    Method method = val.getClass().getMethod("getCode");
                    try {
                        val = method.invoke(val);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                } catch (NoSuchMethodException e) {
                    e.printStackTrace();
                }
            }
            objects.add(val);
        }
        return insertAndGetKey(insertSql, objects.toArray());
    }


    public int update(String whereSql, Map<String, Object> setValues, Object... params) {
        String sql = "update {table} set {setValues} where " + whereSql;
        String setSql = "";
        List<Object> mparams = new ArrayList<Object>();
        boolean isAdd = false;
        if (setValues != null && setValues.size() > 0) {
            for (String key : setValues.keySet()) {
                String upKey = propertyToColumn(key);
                String msql = " " + upKey + " = ? ,";
                setSql += msql;
                mparams.add(setValues.get(key));
                isAdd = true;
            }
        }
        if (isAdd) {
            setSql = setSql.substring(0, setSql.length() - 1);
        }
        sql = sql.replace("{setValues}", setSql);
        if (params != null && params.length > 0)
            for (Object p : params) {
                mparams.add(p);
            }
        return update(sql, mparams.toArray());
    }

    public int delete(String whereSql, Object... params) {
        String sql = "delete from {table} where " + whereSql;
        return update(sql, params);
    }


}
