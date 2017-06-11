package com.lorne.mysql.framework.dao;


import com.lorne.core.framework.model.BaseEntity;
import com.lorne.core.framework.model.Page;

import java.util.List;

/**
 * Created by yuliang on 2017/4/7.
 */
public interface BaseDao<T extends BaseEntity> {


    <T> List<T> findAll();

    <T> Page<T> pageAll(int nowPage, int pageSize);

    <T> List<T> listAll(int nowPage, int pageSize);

    long save(T t);

    int update(T t);

    T getEntityById(Object id);

    int deleteById(Object id);

}
