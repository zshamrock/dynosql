package com.akazlou.dynosql;

import static com.akazlou.dynosql.SQLQuery.Scalar.Operation;
import static com.akazlou.dynosql.SQLQuery.Scalar.Operation.BETWEEN;
import static com.akazlou.dynosql.SQLQuery.Scalar.Operation.BETWEEN_AND;
import static com.akazlou.dynosql.SQLQuery.Scalar.Operation.EQ;
import static com.akazlou.dynosql.SQLQuery.Scalar.Operation.GE;
import static com.akazlou.dynosql.SQLQuery.Scalar.Operation.GT;
import static com.akazlou.dynosql.SQLQuery.Scalar.Operation.LE;
import static com.akazlou.dynosql.SQLQuery.Scalar.Operation.LT;
import static com.akazlou.dynosql.SQLQuery.Scalar.Operation.NE_ANSI;
import static com.akazlou.dynosql.SQLQuery.Scalar.Operation.NE_C;

import java.util.Arrays;
import java.util.Collections;
import java.util.Deque;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import com.akazlou.dynosql.SQLQuery.Expr;
import com.akazlou.dynosql.SQLQuery.Operator;

class SQLParser {
    private static final Pattern SELECT_QUERY_PATTERN = Pattern.compile(
            "^select\\s+(?<columns>[\\w.*,\\s\\-]+)"
                    + "\\s+from\\s+(?<table>[\\w.\\-]+)"
                    + "(\\s+where\\s+(?<conditions>.+?))?;?$",
            Pattern.CASE_INSENSITIVE);

    private static final String COLUMNS_MATCHER_GROUP = "columns";
    private static final String TABLE_MATCHER_GROUP = "table";
    private static final String CONDITIONS_MATCHER_GROUP = "conditions";
    private static final String COLUMNS_SEPARATOR = ",";
    private static final String AS_KEYWORD = "as";

    private static final List<Operation> OPERATIONS = Arrays.asList(
            GE,
            LE,
            NE_ANSI,
            NE_C,
            GT,
            LT,
            EQ,
            BETWEEN,
            BETWEEN_AND
    );

    public Optional<SQLQuery> parse(final String query) {
        final Matcher matcher = SELECT_QUERY_PATTERN.matcher(query.trim());
        if (!matcher.matches()) {
            return Optional.empty();
        }
        final List<SQLQuery.Column> columns = parseColumns(matcher.group(COLUMNS_MATCHER_GROUP).split(COLUMNS_SEPARATOR));
        final String table = matcher.group(TABLE_MATCHER_GROUP).trim();
        final Optional<Expr> conditions = parseConditions(matcher.group(CONDITIONS_MATCHER_GROUP));

        return Optional.of(new SQLQuery(table, columns, conditions.orElse(null)));
    }

    private List<SQLQuery.Column> parseColumns(final String[] columns) {
        return Arrays.stream(columns)
                .map(String::trim)
                .map(column -> {
                    final String columnLower = column.toLowerCase(Locale.ROOT);
                    if (columnLower.contains(AS_KEYWORD)) {
                        final int asKeywordIndex = columnLower.indexOf(AS_KEYWORD);
                        final String name = column.substring(0, asKeywordIndex).trim();
                        final String alias = column.substring(asKeywordIndex + AS_KEYWORD.length())
                                .trim();
                        return new SQLQuery.Column(name, alias);
                    }
                    return new SQLQuery.Column(column);
                })
                .collect(Collectors.toList());
    }

    private Optional<Expr> parseConditions(final String conditions) {
        if (conditions == null) {
            return Optional.empty();
        }
        final Deque<Object> stack = new LinkedList<>();
        final String[] tokens = conditions.trim().split("\\s+");

        boolean isBetweenInProgress = false;
        for (final String token : tokens) {
            final String upperToken = token.toUpperCase(Locale.ROOT);
            final Optional<Operation> maybeOperation = containsOperation(upperToken, isBetweenInProgress);
            final Optional<Operator> maybeOperator = isOperator(upperToken, isBetweenInProgress);
            if (maybeOperation.isPresent()) {
                final Operation operation = maybeOperation.get();
                final List<String> parts = operation == BETWEEN || operation == BETWEEN_AND
                        ? Collections.emptyList()
                        : Arrays.stream(token.split(operation.getSymbol()))
                        .filter(part -> !part.isEmpty())
                        .collect(Collectors.toList());
                if (parts.size() == 2) {
                    stack.addFirst(parts.get(0));
                    stack.addFirst(operation);
                    stack.addFirst(parts.get(1));
                    reduce(stack);
                } else if (parts.size() == 1) {
                    if (token.startsWith(operation.getSymbol())) {
                        stack.addFirst(operation);
                        stack.addFirst(parts.get(0));
                        reduce(stack);
                    } else {
                        stack.addFirst(parts.get(0));
                        stack.addFirst(operation);
                    }
                } else {
                    stack.addFirst(operation);
                }
                if (operation == BETWEEN) {
                    isBetweenInProgress = true;
                }
            } else if (maybeOperator.isPresent()) {
                final Operator operator = maybeOperator.get();
                stack.addFirst(operator);
            } else {
                final Object action = stack.peekFirst();
                stack.addFirst(token);
                if (action instanceof Operation && action != BETWEEN) {
                    reduce(stack);
                    isBetweenInProgress = false;
                }
            }
        }
        return Optional.of((Expr) stack.removeFirst());
    }

    private void reduce(final Deque<Object> stack) {
        final String value2 = (String) stack.removeFirst();
        final Operation operation = (Operation) stack.removeFirst();
        final String value1 = (String) stack.removeFirst();
        final Expr expr;
        if (operation == BETWEEN_AND) {
            // Remove BETWEEN from the stack, and the column name
            expr = reduce((Operation) stack.removeFirst(), (String) stack.removeFirst(), value1, value2);
        } else {
            expr = reduce(operation, value1, value2);
        }
        final Object action = stack.peekFirst();
        if (action instanceof Operator) {
            stack.addFirst(reduce((Operator) stack.removeFirst(), (Expr) stack.removeFirst(), expr));
        } else {
            stack.addFirst(expr);
        }
    }

    private Expr reduce(final Operator operator, final Expr expr1, final Expr expr2) {
        return operator.apply(expr1, expr2);
    }

    private Expr reduce(final Operation operation, final String columnName, final String... value) {
        return operation.apply(columnName, value);
    }

    private Optional<Operator> isOperator(final String upperToken, final boolean isBetweenInProgress) {
        for (final Operator operator : Operator.values()) {
            if (upperToken.equals(operator.name())) {
                if (operator == Operator.AND && isBetweenInProgress) {
                    return Optional.empty();
                }
                return Optional.of(operator);
            }
        }
        return Optional.empty();
    }

    private Optional<Operation> containsOperation(final String upperToken, final boolean isBetweenInProgress) {
        for (final Operation operation : OPERATIONS) {
            if (upperToken.contains(operation.getSymbol())) {
                if (operation == BETWEEN_AND && !isBetweenInProgress) {
                    return Optional.empty();
                }
                return Optional.of(operation);
            }
        }
        return Optional.empty();
    }
}
