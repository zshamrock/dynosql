package com.akazlou.dynosql

import spock.lang.Specification
import spock.lang.Unroll

class SQLParserSpec extends Specification {
    @Unroll
    def "parse simple SELECT query #sql"(
            String sql, String tableName, List<String> columns, List<SQLQuery.Expr> conditions) {
        when:
        def query = new SQLParser().parse(sql).get()

        then:
        query.tableName == tableName
        query.columns == columns
        query.conditions == conditions

        where:
        sql                                               || tableName || columns                  || conditions
        "select * from T"                                 || "T"       || ["*"]                    || []
        "select x, y from db.T"                           || "db.T"    || ["x", "y"]               || []
        "SELECT col1, col2 FROM db_T"                     || "db_T"    || ["col1", "col2"]         || []
        "sElEcT COL_1, Col-2 FrOM db-T"                   || "db-T"    || ["COL_1", "Col-2"]       || []
        "select x as X, y AS y, Col_1 as Col_2, z from T" || "T"       || ["X", "y", "Col_2", "z"] || []
    }
}
