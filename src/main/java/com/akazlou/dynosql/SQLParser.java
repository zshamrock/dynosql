package com.akazlou.dynosql;

import static com.akazlou.dynosql.SQLQuery.Scalar.Operation;
import static com.akazlou.dynosql.SQLQuery.Scalar.Operation.BETWEEN;
import static com.akazlou.dynosql.SQLQuery.Scalar.Operation.EQ;
import static com.akazlou.dynosql.SQLQuery.Scalar.Operation.GE;
import static com.akazlou.dynosql.SQLQuery.Scalar.Operation.GT;
import static com.akazlou.dynosql.SQLQuery.Scalar.Operation.IN;
import static com.akazlou.dynosql.SQLQuery.Scalar.Operation.LE;
import static com.akazlou.dynosql.SQLQuery.Scalar.Operation.LT;
import static com.akazlou.dynosql.SQLQuery.Scalar.Operation.NE_ANSI;
import static com.akazlou.dynosql.SQLQuery.Scalar.Operation.NE_C;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Deque;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import com.akazlou.dynosql.SQLQuery.Expr;
import com.akazlou.dynosql.SQLQuery.Operator;

/**
 * SQL parser.
 */
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
            IN
    );

    private static final char SINGLE_QUOTE = '\'';
    private static final char SPACE = ' ';
    private static final char COMMA = ',';
    private static final char OPEN_PARENS = '(';
    private static final char CLOSED_PARENS = ')';
    private static final char EQUAL = '=';
    private static final char GREATER = '>';
    private static final char LESS = '<';
    private static final char NOT = '!';

    private final List<String> functions;

    SQLParser() {
        this(new ArrayList<>());
    }

    SQLParser(final List<String> functions) {
        this.functions = Collections.unmodifiableList(functions);
    }

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
        final Deque<Object> tokens = new LinkedList<>();
        final Deque<Context> contexts = new LinkedList<>();
        final Deque<Character> parens = new LinkedList<>();
        final StringBuilder builder = new StringBuilder();
        // Append space as the end terminator to process the last token
        final String conditionsWithTerminator = conditions + SPACE;
        for (int i = 0; i < conditionsWithTerminator.length(); i++) {
            final Context context = contexts.peekFirst();
            final char c = conditionsWithTerminator.charAt(i);
            final char next;
            final String token;
            final boolean reduced;
            switch (c) {
                case SPACE:
                    if (isQuoteContext(context)) {
                        builder.append(c);
                        continue;
                    }
                    token = builder.toString();
                    builder.setLength(0);
                    if (token.isEmpty()) {
                        continue;
                    }
                    parse(token, context, contexts).ifPresent(tokens::addFirst);
                    if (context != null && parens.isEmpty()) {
                        reduced = tryReduce(context, tokens);
                        if (reduced) {
                            contexts.removeFirst();
                        }
                    }
                    continue;
                case SINGLE_QUOTE:
                    builder.append(c);
                    if (isQuoteContext(context)) {
                        contexts.removeFirst();
                    } else {
                        contexts.addFirst(Context.SINGLE_QUOTE);
                    }
                    continue;
                case COMMA:
                    if (isQuoteContext(context)) {
                        builder.append(c);
                        continue;
                    }
                    if (isInContext(context)) {
                        token = builder.toString();
                        builder.setLength(0);
                        if (!token.isEmpty()) {
                            tokens.addFirst(token);
                        }
                        continue;
                    }
                    throw new IllegalArgumentException(
                            String.format("Could not parse WHERE conditions, unexpected %s: %s", COMMA, conditions));
                case EQUAL:
                    if (isQuoteContext(context)) {
                        builder.append(c);
                        continue;
                    }
                    token = builder.toString();
                    builder.setLength(0);
                    if (!token.isEmpty()) {
                        tokens.addFirst(token);
                    }
                    contexts.addFirst(Context.BASIC_OPERATION_CONTEXT);
                    tokens.addFirst(EQ);
                    continue;
                case NOT:
                    if (isQuoteContext(context)) {
                        builder.append(c);
                        continue;
                    }
                    next = conditionsWithTerminator.charAt(i + 1);
                    if (next == EQUAL) {
                        token = builder.toString();
                        builder.setLength(0);
                        if (!token.isEmpty()) {
                            tokens.addFirst(token);
                        }
                        tokens.addFirst(NE_C);
                        contexts.addFirst(Context.BASIC_OPERATION_CONTEXT);
                        i++;
                        continue;
                    } else {
                        throw new IllegalArgumentException(
                                String.format("Could not parse WHERE conditions, unexpected %s: %s", NOT, conditions));
                    }
                case GREATER:
                    if (isQuoteContext(context)) {
                        builder.append(c);
                        continue;
                    }
                    token = builder.toString();
                    builder.setLength(0);
                    if (!token.isEmpty()) {
                        tokens.addFirst(token);
                    }
                    next = conditionsWithTerminator.charAt(i + 1);
                    if (next == EQUAL) {
                        tokens.addFirst(GE);
                        i++;
                    } else {
                        tokens.addFirst(GT);
                    }
                    contexts.addFirst(Context.BASIC_OPERATION_CONTEXT);
                    continue;
                case LESS:
                    if (isQuoteContext(context)) {
                        builder.append(c);
                        continue;
                    }
                    token = builder.toString();
                    builder.setLength(0);
                    if (!token.isEmpty()) {
                        tokens.addFirst(token);
                    }
                    next = conditionsWithTerminator.charAt(i + 1);
                    if (next == EQUAL) {
                        tokens.addFirst(LE);
                        i++;
                    } else if (next == GREATER) {
                        i++;
                        tokens.addFirst(NE_ANSI);
                    } else {
                        tokens.addFirst(LT);
                    }
                    contexts.addFirst(Context.BASIC_OPERATION_CONTEXT);
                    continue;
                case OPEN_PARENS:
                    if (isQuoteContext(context)) {
                        builder.append(c);
                        continue;
                    }
                    parens.addFirst(OPEN_PARENS);
                    continue;
                case CLOSED_PARENS:
                    if (isQuoteContext(context)) {
                        builder.append(c);
                        continue;
                    }
                    if (parens.isEmpty() || parens.peekFirst() != OPEN_PARENS) {
                        throw new IllegalArgumentException(
                                String.format("Could not parse WHERE conditions, no matching open parens for the "
                                                + "closed parens: %s",
                                        conditions));
                    }
                    parens.removeFirst();
                    token = builder.toString();
                    builder.setLength(0);
                    if (!token.isEmpty()) {
                        tokens.addFirst(token);
                    }
                    if (parens.isEmpty()) {
                        reduced = tryReduce(context, tokens);
                        if (reduced) {
                            contexts.removeFirst();
                        }
                    }
                    continue;
                default:
                    builder.append(c);
            }
        }

        if (!parens.isEmpty()) {
            throw new IllegalArgumentException(
                    String.format("Could not parse WHERE conditions, no matching parens: %s", conditions));
        }
        if (tokens.size() != 1) {
            throw new IllegalArgumentException(
                    String.format("Could not parse WHERE conditions, was not reduced to the single expression: %s",
                            conditions));
        }

        return Optional.of((Expr) tokens.removeFirst());
    }

    private boolean tryReduce(final Context context, final Deque<Object> tokens) {
        final Operation operation;
        final String columnName;
        final Expr expr;
        final boolean reduced;
        switch (context) {
            case BASIC_OPERATION_CONTEXT:
                final String value = (String) tokens.removeFirst();
                operation = (Operation) tokens.removeFirst();
                columnName = (String) tokens.removeFirst();
                expr = reduce(operation, columnName, value);
                reduced = true;
                break;
            case IN:
                final Set<String> inValues = new HashSet<>();
                while (!(tokens.peekFirst() instanceof Operation)) {
                    inValues.add((String) tokens.removeFirst());
                }
                operation = (Operation) tokens.removeFirst();
                verify(operation, IN);
                columnName = (String) tokens.removeFirst();
                expr = reduce(operation, columnName, inValues.toArray(new String[0]));
                reduced = true;
                break;
            case BETWEEN:
                final Deque<Object> betweenValues = new LinkedList<>();
                while (!(tokens.peekFirst() instanceof Operation)) {
                    betweenValues.addFirst(tokens.removeFirst());
                }
                if (betweenValues.size() != 2) {
                    betweenValues.forEach(tokens::addFirst);
                    return false;
                }
                operation = (Operation) tokens.removeFirst();
                verify(operation, BETWEEN);
                columnName = (String) tokens.removeFirst();
                expr = reduce(
                        operation,
                        columnName,
                        (String) betweenValues.removeFirst(),
                        (String) betweenValues.removeFirst());
                reduced = true;
                break;
            default:
                throw new IllegalArgumentException(String.format("Unsupported context %s for the reduction", context));
        }
        final Object action = tokens.peekFirst();
        if (action instanceof Operator) {
            tokens.addFirst(reduce((Operator) tokens.removeFirst(), (Expr) tokens.removeFirst(), expr));
        } else {
            tokens.addFirst(expr);
        }
        return reduced;
    }

    private void verify(final Operation operation, final Operation expected) {
        if (operation != expected) {
            throw new IllegalArgumentException(
                    String.format("Expect %s keyword, but got %s", expected.getSymbol(), operation));
        }
    }

    private Optional<?> parse(final String token,
                              final Context context,
                              final Deque<Context> contexts) {
        final Optional<Operation> operation = isOperation(token, context);
        if (operation.isPresent()) {
            switch (operation.get()) {
                case BETWEEN:
                    contexts.addFirst(Context.BETWEEN);
                    break;
                case IN:
                    contexts.addFirst(Context.IN);
                    break;
            }
            return operation;
        }
        final Optional<Operator> operator = isOperator(token, context);
        if (operator.isPresent()) {
            return operator;
        }
        final String tokenUpper = token.toUpperCase(Locale.ROOT);
        if (tokenUpper.equals(Operator.AND.name()) && isBetweenContext(context)) {
            return Optional.empty();
        }
        return Optional.of(token);
    }

    private boolean isQuoteContext(final Context context) {
        return context == Context.SINGLE_QUOTE;
    }

    private boolean isSpecial(final Operation operation) {
        return operation == BETWEEN || operation == IN;
    }

    private Expr reduce(final Operator operator, final Expr expr1, final Expr expr2) {
        return operator.apply(expr1, expr2);
    }

    private Expr reduce(final Operation operation, final String columnName, final String... value) {
        return operation.apply(columnName, value);
    }

    private Optional<Operator> isOperator(final String token, final Context context) {
        final String tokenUpper = token.toUpperCase(Locale.ROOT);
        for (final Operator operator : Operator.values()) {
            if (tokenUpper.equals(operator.name())) {
                if (operator == Operator.AND && isBetweenContext(context)) {
                    return Optional.empty();
                }
                return Optional.of(operator);
            }
        }
        return Optional.empty();
    }

    private Optional<Operation> isOperation(final String token, final Context context) {
        final String tokenUpper = token.toUpperCase(Locale.ROOT);
        for (final Operation operation : OPERATIONS) {
            if (tokenUpper.equals(operation.getSymbol())) {
                return Optional.of(operation);
            }
        }
        return Optional.empty();
    }

    private boolean isBetweenContext(final Context context) {
        return context == Context.BETWEEN;
    }

    private boolean isInContext(final Context context) {
        return context == Context.IN;
    }

    private enum Context {
        BETWEEN,
        SINGLE_QUOTE,
        BASIC_OPERATION_CONTEXT,
        IN
    }
}
