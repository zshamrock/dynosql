package com.akazlou.dynosql

import static com.akazlou.dynosql.SQLQuery.Column
import static com.akazlou.dynosql.SQLQuery.Expr
import static com.akazlou.dynosql.SQLQuery.Scalar.Operation

import spock.lang.Specification
import spock.lang.Unroll

class SQLParserSpec extends Specification {
    @Unroll
    def "parse simple SELECT query #sql"(
            String sql, String tableName, List<Column> columns) {
        when:
        def query = new SQLParser().parse(sql).get()

        then:
        query.tableName == tableName
        query.columns == columns
        !query.conditions.isPresent()

        where:
        sql                                               || tableName || columns
        "select * from T"                                 || "T"       || [new Column("*")]
        "select x, y from db.T"                           || "db.T"    || [new Column("x"),
                                                                           new Column("y")]
        "SELECT col1, col2 FROM db_T"                     || "db_T"    || [new Column("col1"),
                                                                           new Column("col2")]
        "sElEcT COL_1, Col-2 FrOM db-T"                   || "db-T"    || [new Column("COL_1"),
                                                                           new Column("Col-2")]
        "select x as X, y AS y, Col_1 as Col_2, z from T" || "T"       || [new Column("x", "X"),
                                                                           new Column("y", "y"),
                                                                           new Column("Col_1", "Col_2"),
                                                                           new Column("z")]
    }

    @Unroll
    def "parse simple where conditions #sql"(String sql, Expr conditions) {
        when:
        def query = new SQLParser().parse(sql).get()

        then:
        query.getConditions().isPresent()
        query.getConditions().get() == conditions

        where:
        sql                             || conditions
        // =
        "select * from T where id = 1"  || new SQLQuery.Scalar<String>("id", "1", Operation.EQ)
        "select * from T where id=1"    || new SQLQuery.Scalar<String>("id", "1", Operation.EQ)
        "select * from T where id= 1"   || new SQLQuery.Scalar<String>("id", "1", Operation.EQ)
        "select * from T where id =1"   || new SQLQuery.Scalar<String>("id", "1", Operation.EQ)

        // >
        "select * from T where id > 1"  || new SQLQuery.Scalar<String>("id", "1", Operation.GT)
        "select * from T where id>1"    || new SQLQuery.Scalar<String>("id", "1", Operation.GT)
        "select * from T where id> 1"   || new SQLQuery.Scalar<String>("id", "1", Operation.GT)
        "select * from T where id >1"   || new SQLQuery.Scalar<String>("id", "1", Operation.GT)

        // >=
        "select * from T where id >= 1" || new SQLQuery.Scalar<String>("id", "1", Operation.GE)
        "select * from T where id>=1"   || new SQLQuery.Scalar<String>("id", "1", Operation.GE)
        "select * from T where id>= 1"  || new SQLQuery.Scalar<String>("id", "1", Operation.GE)
        "select * from T where id >=1"  || new SQLQuery.Scalar<String>("id", "1", Operation.GE)

        // <
        "select * from T where id < 1"  || new SQLQuery.Scalar<String>("id", "1", Operation.LT)
        "select * from T where id<1"    || new SQLQuery.Scalar<String>("id", "1", Operation.LT)
        "select * from T where id< 1"   || new SQLQuery.Scalar<String>("id", "1", Operation.LT)
        "select * from T where id <1"   || new SQLQuery.Scalar<String>("id", "1", Operation.LT)

        // <=
        "select * from T where id <= 1" || new SQLQuery.Scalar<String>("id", "1", Operation.LE)
        "select * from T where id<=1"   || new SQLQuery.Scalar<String>("id", "1", Operation.LE)
        "select * from T where id<= 1"  || new SQLQuery.Scalar<String>("id", "1", Operation.LE)
        "select * from T where id <=1"  || new SQLQuery.Scalar<String>("id", "1", Operation.LE)

        // <>
        "select * from T where id <> 1" || new SQLQuery.Scalar<String>("id", "1", Operation.NE_ANSI)
        "select * from T where id<>1"   || new SQLQuery.Scalar<String>("id", "1", Operation.NE_ANSI)
        "select * from T where id<> 1"  || new SQLQuery.Scalar<String>("id", "1", Operation.NE_ANSI)
        "select * from T where id <>1"  || new SQLQuery.Scalar<String>("id", "1", Operation.NE_ANSI)

        // !=
        "select * from T where id != 1" || new SQLQuery.Scalar<String>("id", "1", Operation.NE_C)
        "select * from T where id!=1"   || new SQLQuery.Scalar<String>("id", "1", Operation.NE_C)
        "select * from T where id!= 1"  || new SQLQuery.Scalar<String>("id", "1", Operation.NE_C)
        "select * from T where id !=1"  || new SQLQuery.Scalar<String>("id", "1", Operation.NE_C)
    }
}
