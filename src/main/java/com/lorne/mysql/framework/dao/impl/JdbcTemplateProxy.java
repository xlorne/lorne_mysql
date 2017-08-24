package com.lorne.mysql.framework.dao.impl;


import org.springframework.jdbc.core.JdbcTemplate;

public class JdbcTemplateProxy {


    private JdbcTemplate jdbcTemplate;


    public JdbcTemplate getJdbcTemplate() {
        return jdbcTemplate;
    }

    public void setJdbcTemplate(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }
}
