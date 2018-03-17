package com.akazlou.dynosql;

import java.util.List;
import java.util.Objects;

class SQLQuery {

    private final String tableName;
    private final List<String> columns;
    private final List<Expr> conditions;

    SQLQuery(final String tableName, final List<String> columns, final List<Expr> conditions) {
        this.tableName = tableName;
        this.columns = columns;
        this.conditions = conditions;
    }

    String getTableName() {
        return tableName;
    }

    List<String> getColumns() {
        return columns;
    }

    List<Expr> getConditions() {
        return conditions;
    }

    public enum Type {
        SELECT
    }

    interface Expr {
    }

    static final class AndExpr implements Expr {
        private final Expr ex1;
        private final Expr ex2;

        AndExpr(final Expr ex1, final Expr ex2) {
            this.ex1 = ex1;
            this.ex2 = ex2;
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof AndExpr)) {
                return false;
            }
            final AndExpr andExpr = (AndExpr) o;
            return Objects.equals(ex1, andExpr.ex1) &&
                    Objects.equals(ex2, andExpr.ex2);
        }

        @Override
        public int hashCode() {
            return Objects.hash(ex1, ex2);
        }
    }

    static final class OrExpr implements Expr {
        private final Expr ex1;
        private final Expr ex2;

        OrExpr(final Expr ex1, final Expr ex2) {
            this.ex1 = ex1;
            this.ex2 = ex2;
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof OrExpr)) {
                return false;
            }
            final OrExpr orExpr = (OrExpr) o;
            return Objects.equals(ex1, orExpr.ex1) &&
                    Objects.equals(ex2, orExpr.ex2);
        }

        @Override
        public int hashCode() {

            return Objects.hash(ex1, ex2);
        }
    }

    static final class Scalar implements Expr {
        private final String columnName;
        private final String value;
        private final Operation operation;

        Scalar(final String columnName, final String value, final Operation operation) {
            this.columnName = columnName;
            this.value = value;
            this.operation = operation;
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof Scalar)) {
                return false;
            }
            final Scalar scalar = (Scalar) o;
            return Objects.equals(columnName, scalar.columnName) &&
                    Objects.equals(value, scalar.value) &&
                    operation == scalar.operation;
        }

        @Override
        public int hashCode() {
            return Objects.hash(columnName, value, operation);
        }

        enum Operation {
            EQ, GE, GT, LT, LE, BETWEEN // LIKE?
        }
    }
}
