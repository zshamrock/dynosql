#!/bin/bash
export CLASSPATH=${CLASSPATH}:target/classes && \
    java org.antlr.v4.gui.TestRig com.akazlou.dynosql.antlr4.plsql.PlSql sql_script -gui
