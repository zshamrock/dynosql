package com.akazlou.dynosql;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

class SQLQuery {

    private final String tableName;
    private final List<Column> columns;
    private final Expr conditions;

    SQLQuery(final String tableName, final List<Column> columns, final Expr conditions) {
        this.tableName = tableName;
        this.columns = columns;
        this.conditions = conditions;
    }

    String getTableName() {
        return tableName;
    }

    List<Column> getColumns() {
        return columns;
    }

    Optional<Expr> getConditions() {
        return Optional.ofNullable(conditions);
    }

    public enum Type {
        SELECT
    }

    static final class Column {
        private final String name;
        private final String alias;

        Column(final String name) {
            this(name, null);
        }

        Column(final String name, final String alias) {
            this.name = name;
            this.alias = alias;
        }

        String getName() {
            return name;
        }

        Optional<String> getAlias() {
            return Optional.ofNullable(alias);
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof Column)) {
                return false;
            }
            final Column column = (Column) o;
            return Objects.equals(name, column.name) &&
                    Objects.equals(alias, column.alias);
        }

        @Override
        public int hashCode() {
            return Objects.hash(name, alias);
        }
    }

    interface Expr {
    }

    enum Operator {
        OR, AND;

        Expr apply(final Expr expr1, final Expr expr2) {
            switch (this) {
                case OR:
                    return new OrExpr(expr1, expr2);
                case AND:
                    return new AndExpr(expr1, expr2);
                default:
                    throw new UnsupportedOperationException("Operator is not supported");
            }
        }
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

    static final class Scalar<T> implements Expr {
        private final String columnName;
        private final T value;
        private final Operation operation;

        Scalar(final String columnName, final T value, final Operation operation) {
            this.columnName = columnName;
            this.value = value;
            this.operation = operation;
        }

        String getColumnName() {
            return columnName;
        }

        T getValue() {
            return value;
        }

        Operation getOperation() {
            return operation;
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
            GE(">="),
            LE("<="),
            NE_ANSI("<>"),
            NE_C("!="),
            GT(">"),
            LT("<"),
            EQ("="),
            BETWEEN("BETWEEN"),
            BETWEEN_AND("AND"),
            IS_NULL("IS NULL"),
            IS_NOT_NULL("IS NOT NULL"),
            IN("IN");
            // LIKE?

            private final String symbol;

            Operation(final String symbol) {
                this.symbol = symbol;
            }

            String getSymbol() {
                return symbol;
            }

            Expr apply(final String column, final String... value) {
                switch (this) {
                    case GE:
                        // fall through
                    case LE:
                        // fall through
                    case NE_ANSI:
                        // fall through
                    case NE_C:
                        // fall through
                    case GT:
                        // fall through
                    case LT:
                        // fall through
                    case EQ:
                        return new Scalar<>(column, value[0], this);
                    case BETWEEN:
                        return new Scalar<>(column, new Between<>(value[0], value[1]), this);
                    default:
                        throw new UnsupportedOperationException(
                                String.format("Operation %s is not supported", this));
                }
            }
        }

        static final class Between<T> {
            private final T from;
            private final T to;

            Between(final T from, final T to) {
                this.from = from;
                this.to = to;
            }

            @Override
            public boolean equals(final Object o) {
                if (this == o) {
                    return true;
                }
                if (!(o instanceof Between)) {
                    return false;
                }
                final Between<?> between = (Between<?>) o;
                return Objects.equals(from, between.from) &&
                        Objects.equals(to, between.to);
            }

            @Override
            public int hashCode() {
                return Objects.hash(from, to);
            }
        }
    }
}
