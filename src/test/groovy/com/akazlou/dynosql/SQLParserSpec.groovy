package com.akazlou.dynosql

import static com.akazlou.dynosql.SQLQuery.Column
import static com.akazlou.dynosql.SQLQuery.Expr
import static com.akazlou.dynosql.SQLQuery.Scalar.Between
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
//        "select * from T where id=1"    || new SQLQuery.Scalar<String>("id", "1", Operation.EQ)
//        "select * from T where id= 1"   || new SQLQuery.Scalar<String>("id", "1", Operation.EQ)
//        "select * from T where id =1"   || new SQLQuery.Scalar<String>("id", "1", Operation.EQ)
//
//        // >
//        "select * from T where id > 1"  || new SQLQuery.Scalar<String>("id", "1", Operation.GT)
//        "select * from T where id>1"    || new SQLQuery.Scalar<String>("id", "1", Operation.GT)
//        "select * from T where id> 1"   || new SQLQuery.Scalar<String>("id", "1", Operation.GT)
//        "select * from T where id >1"   || new SQLQuery.Scalar<String>("id", "1", Operation.GT)
//
//        // >=
//        "select * from T where id >= 1" || new SQLQuery.Scalar<String>("id", "1", Operation.GE)
//        "select * from T where id>=1"   || new SQLQuery.Scalar<String>("id", "1", Operation.GE)
//        "select * from T where id>= 1"  || new SQLQuery.Scalar<String>("id", "1", Operation.GE)
//        "select * from T where id >=1"  || new SQLQuery.Scalar<String>("id", "1", Operation.GE)
//
//        // <
//        "select * from T where id < 1"  || new SQLQuery.Scalar<String>("id", "1", Operation.LT)
//        "select * from T where id<1"    || new SQLQuery.Scalar<String>("id", "1", Operation.LT)
//        "select * from T where id< 1"   || new SQLQuery.Scalar<String>("id", "1", Operation.LT)
//        "select * from T where id <1"   || new SQLQuery.Scalar<String>("id", "1", Operation.LT)
//
//        // <=
//        "select * from T where id <= 1" || new SQLQuery.Scalar<String>("id", "1", Operation.LE)
//        "select * from T where id<=1"   || new SQLQuery.Scalar<String>("id", "1", Operation.LE)
//        "select * from T where id<= 1"  || new SQLQuery.Scalar<String>("id", "1", Operation.LE)
//        "select * from T where id <=1"  || new SQLQuery.Scalar<String>("id", "1", Operation.LE)
//
//        // <>
//        "select * from T where id <> 1" || new SQLQuery.Scalar<String>("id", "1", Operation.NE_ANSI)
//        "select * from T where id<>1"   || new SQLQuery.Scalar<String>("id", "1", Operation.NE_ANSI)
//        "select * from T where id<> 1"  || new SQLQuery.Scalar<String>("id", "1", Operation.NE_ANSI)
//        "select * from T where id <>1"  || new SQLQuery.Scalar<String>("id", "1", Operation.NE_ANSI)
//
//        // !=
//        "select * from T where id != 1" || new SQLQuery.Scalar<String>("id", "1", Operation.NE_C)
//        "select * from T where id!=1"   || new SQLQuery.Scalar<String>("id", "1", Operation.NE_C)
//        "select * from T where id!= 1"  || new SQLQuery.Scalar<String>("id", "1", Operation.NE_C)
//        "select * from T where id !=1"  || new SQLQuery.Scalar<String>("id", "1", Operation.NE_C)
    }

    @Unroll
    def "parse single quotes in simple where conditions #sql"(String sql, Expr conditions) {
        when:
        def query = new SQLParser().parse(sql).get()

        then:
        query.getConditions().isPresent()
        query.getConditions().get() == conditions

        where:
        sql                               || conditions
        // =
        "select * from T where id = '(()1 ,)'"  || new SQLQuery.Scalar<String>("id", "'(()1 ,)'", Operation.EQ)
        "select * from T where id='(()1 ,)'"    || new SQLQuery.Scalar<String>("id", "'(()1 ,)'", Operation.EQ)
        "select * from T where id= '(()1 ,)'"   || new SQLQuery.Scalar<String>("id", "'(()1 ,)'", Operation.EQ)
        "select * from T where id ='(()1 ,)'"   || new SQLQuery.Scalar<String>("id", "'(()1 ,)'", Operation.EQ)

        // >
        "select * from T where id > '(()1 ,)'"  || new SQLQuery.Scalar<String>("id", "'(()1 ,)'", Operation.GT)
        "select * from T where id>'(()1 ,)'"    || new SQLQuery.Scalar<String>("id", "'(()1 ,)'", Operation.GT)
        "select * from T where id> '(()1 ,)'"   || new SQLQuery.Scalar<String>("id", "'(()1 ,)'", Operation.GT)
        "select * from T where id >'(()1 ,)'"   || new SQLQuery.Scalar<String>("id", "'(()1 ,)'", Operation.GT)

        // >=
        "select * from T where id >= '(()1 ,)'" || new SQLQuery.Scalar<String>("id", "'(()1 ,)'", Operation.GE)
        "select * from T where id>='(()1 ,)'"   || new SQLQuery.Scalar<String>("id", "'(()1 ,)'", Operation.GE)
        "select * from T where id>= '(()1 ,)'"  || new SQLQuery.Scalar<String>("id", "'(()1 ,)'", Operation.GE)
        "select * from T where id >='(()1 ,)'"  || new SQLQuery.Scalar<String>("id", "'(()1 ,)'", Operation.GE)

        // <
        "select * from T where id < '(()1 ,)'"  || new SQLQuery.Scalar<String>("id", "'(()1 ,)'", Operation.LT)
        "select * from T where id<'(()1 ,)'"    || new SQLQuery.Scalar<String>("id", "'(()1 ,)'", Operation.LT)
        "select * from T where id< '(()1 ,)'"   || new SQLQuery.Scalar<String>("id", "'(()1 ,)'", Operation.LT)
        "select * from T where id <'(()1 ,)'"   || new SQLQuery.Scalar<String>("id", "'(()1 ,)'", Operation.LT)

        // <=
        "select * from T where id <= '(()1 ,)'" || new SQLQuery.Scalar<String>("id", "'(()1 ,)'", Operation.LE)
        "select * from T where id<='(()1 ,)'"   || new SQLQuery.Scalar<String>("id", "'(()1 ,)'", Operation.LE)
        "select * from T where id<= '(()1 ,)'"  || new SQLQuery.Scalar<String>("id", "'(()1 ,)'", Operation.LE)
        "select * from T where id <='(()1 ,)'"  || new SQLQuery.Scalar<String>("id", "'(()1 ,)'", Operation.LE)

        // <>
        "select * from T where id <> '(()1 ,)'" || new SQLQuery.Scalar<String>("id", "'(()1 ,)'", Operation.NE_ANSI)
        "select * from T where id<>'(()1 ,)'"   || new SQLQuery.Scalar<String>("id", "'(()1 ,)'", Operation.NE_ANSI)
        "select * from T where id<> '(()1 ,)'"  || new SQLQuery.Scalar<String>("id", "'(()1 ,)'", Operation.NE_ANSI)
        "select * from T where id <>'(()1 ,)'"  || new SQLQuery.Scalar<String>("id", "'(()1 ,)'", Operation.NE_ANSI)

        // !=
        "select * from T where id != '(()1 ,)'" || new SQLQuery.Scalar<String>("id", "'(()1 ,)'", Operation.NE_C)
        "select * from T where id!='(()1 ,)'"   || new SQLQuery.Scalar<String>("id", "'(()1 ,)'", Operation.NE_C)
        "select * from T where id!= '(()1 ,)'"  || new SQLQuery.Scalar<String>("id", "'(()1 ,)'", Operation.NE_C)
        "select * from T where id !='(()1 ,)'"  || new SQLQuery.Scalar<String>("id", "'(()1 ,)'", Operation.NE_C)
    }

    @Unroll
    def "parse conditional where conditions #sql"(String sql, Expr conditions) {
        when:
        def query = new SQLParser().parse(sql).get()

        then:
        query.getConditions().isPresent()
        query.getConditions().get() == conditions

        where:
        sql                                                              || conditions
        "select * from T where x = 1 and y > 5"                          ||
                new SQLQuery.AndExpr(
                        new SQLQuery.Scalar("x", "1", Operation.EQ),
                        new SQLQuery.Scalar("y", "5", Operation.GT))
        "select * from T where x <> 1 OR y <= 5"                         ||
                new SQLQuery.OrExpr(
                        new SQLQuery.Scalar("x", "1", Operation.NE_ANSI),
                        new SQLQuery.Scalar("y", "5", Operation.LE))
        "select * from T where x = 1 and y > 5 and z != 3"               ||
                new SQLQuery.AndExpr(
                        new SQLQuery.AndExpr(new SQLQuery.Scalar("x", "1", Operation.EQ),
                                new SQLQuery.Scalar("y", "5", Operation.GT)),
                        new SQLQuery.Scalar("z", "3", Operation.NE_C))
        "select * from T where x <> 1 OR y <= 5 AnD z = x"               ||
                new SQLQuery.AndExpr(
                        new SQLQuery.OrExpr(
                                new SQLQuery.Scalar("x", "1", Operation.NE_ANSI),
                                new SQLQuery.Scalar("y", "5", Operation.LE)),
                        new SQLQuery.Scalar("z", "x", Operation.EQ))
        "select * from T where x <> 1 OR y <= 5 AnD z = x and Col_1 > 7" ||
                new SQLQuery.AndExpr(new SQLQuery.AndExpr(
                        new SQLQuery.OrExpr(
                                new SQLQuery.Scalar("x", "1", Operation.NE_ANSI),
                                new SQLQuery.Scalar("y", "5", Operation.LE)),
                        new SQLQuery.Scalar("z", "x", Operation.EQ)),
                        new SQLQuery.Scalar("Col_1", "7", Operation.GT))
    }

    @Unroll
    def "parse BETWEEN in where conditions #sql"(String sql, Expr conditions) {
        when:
        def query = new SQLParser().parse(sql).get()

        then:
        query.getConditions().isPresent()
        query.getConditions().get() == conditions

        where:
        sql                                                                                || conditions
        "select * from T where x between 1 and 5"                                          ||
                new SQLQuery.Scalar("x", new Between("1", "5"), Operation.BETWEEN)
        "select * from T where x between 1 and 5 and t.Timestamp-MS beTween 1000 AnD 2000" ||
                new SQLQuery.AndExpr(
                        new SQLQuery.Scalar("x", new Between("1", "5"), Operation.BETWEEN),
                        new SQLQuery.Scalar("t.Timestamp-MS", new Between("1000", "2000"), Operation.BETWEEN))
        "select * from T where x between 1 and 5 or y BETWEEN -10 AND -5"                  ||
                new SQLQuery.OrExpr(
                        new SQLQuery.Scalar("x", new Between("1", "5"), Operation.BETWEEN),
                        new SQLQuery.Scalar("y", new Between("-10", "-5"), Operation.BETWEEN))
    }

    @Unroll
    def "parse IN in where conditions #sql"(String sql, Expr conditions) {
        when:
        def query = new SQLParser().parse(sql).get()

        then:
        query.getConditions().isPresent()
        query.getConditions().get() == conditions

        where:
        sql                                                                                           || conditions
        "select * from T where x in (1)"                                                              ||
                new SQLQuery.Scalar("x", new SQLQuery.Scalar.In(["1"] as Set), Operation.IN)
        "select * from T where x in ( 1 )"                                                            ||
                new SQLQuery.Scalar("x", new SQLQuery.Scalar.In(["1"] as Set), Operation.IN)
        "select * from T where x in (1, 2)"                                                           ||
                new SQLQuery.Scalar("x", new SQLQuery.Scalar.In(["1", "2"] as Set), Operation.IN)
        "select * from T where x in ( 1, 2 )"                                                         ||
                new SQLQuery.Scalar("x", new SQLQuery.Scalar.In(["1", "2"] as Set), Operation.IN)
        "select * from T where x in ( 1 , 2 )"                                                        ||
                new SQLQuery.Scalar("x", new SQLQuery.Scalar.In(["1", "2"] as Set), Operation.IN)
        "select * from T where x in (1,2)"                                                            ||
                new SQLQuery.Scalar("x", new SQLQuery.Scalar.In(["1", "2"] as Set), Operation.IN)
        "select * from T where x in (1, 2) and y in (1, 2, 3, 4, 5)"                                  ||
                new SQLQuery.AndExpr(
                        new SQLQuery.Scalar("x", new SQLQuery.Scalar.In(["1", "2"] as Set), Operation.IN),
                        new SQLQuery.Scalar("y", new SQLQuery.Scalar.In(["1", "2", "3", "4", "5"] as Set), Operation.IN))
        "select * from T where z = 5 or x in (1, 2) and y between 3 and 4 or a in (5, 6, 7) or b > 5" ||
                new SQLQuery.OrExpr(
                        new SQLQuery.OrExpr(
                                new SQLQuery.AndExpr(
                                        new SQLQuery.OrExpr(
                                                new SQLQuery.Scalar("z", "5", Operation.EQ),
                                                new SQLQuery.Scalar("x", new SQLQuery.Scalar.In(["1", "2"] as Set), Operation.IN)),
                                        new SQLQuery.Scalar("y", new Between("3", "4"), Operation.BETWEEN)),
                                new SQLQuery.Scalar("a", new SQLQuery.Scalar.In(["5", "6", "7"] as Set), Operation.IN)),
                        new SQLQuery.Scalar("b", "5", Operation.GT))
    }
}
