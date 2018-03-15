#!/bin/bash
export CLASSPATH=${CLASSPATH}:target/classes && \
    java org.antlr.v4.gui.TestRig com.akazlou.dynosql.antlr4.mysql.MySql root -gui
