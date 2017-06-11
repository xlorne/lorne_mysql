package com.lorne.mysql.framework.dao.impl;


import com.lorne.core.framework.model.BaseEntity;
import com.lorne.core.framework.model.Page;
import com.lorne.mysql.framework.dao.BaseDao;

import java.util.List;

/**
 * Created by yuliang on 2017/4/7.
 */
public class BaseDaoImpl<T extends BaseEntity>  extends BaseJdbcTemplate<T> implements BaseDao<T> {



    @Override
    public <T> List<T> findAll() {
        return queryForBeanList("select * from {table}");
    }

    @Override
    public <T> Page<T> pageAll(int nowPage, int pageSize) {
        String sql = "select * from {table}";
        return pageForBeanList(sql, nowPage, pageSize);
    }

    @Override
    public <T> List<T> listAll(int nowPage, int pageSize) {
        String sql = "select * from {table}";
        return listForBeanList(sql, nowPage, pageSize);
    }

    @Override
    public long save(T t) {
        return super.save(t);
    }

    @Override
    public int update(T t) {
        return super.update(t);
    }

    @Override
    public T getEntityById(Object id) {
        String sql = "select * from {table} where {id} = ?";
        sql = sql.replace("{id}", idName);
        return queryForBean(sql, id);
    }

    @Override
    public int deleteById(Object id) {
        String sql = "delete from {table} where {id} = ?";
        sql = sql.replace("{id}", idName);
        return update(sql, id);
    }
}
