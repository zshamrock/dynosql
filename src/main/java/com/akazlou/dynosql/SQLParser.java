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
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
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
            switch (c) {
                case SPACE:
                    if (isQuoteContext(context)) {
                        builder.append(c);
                        continue;
                    }
                    // SPACE currently the single char if not in the quote context triggers token parse and reduction
                    final String token = builder.toString();
                    builder.setLength(0);
                    if (!token.isEmpty()) {
                        tokens.addFirst(parse(token, context));
                    }
                    reduce(tokens);
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
                        tokens.addFirst(builder.toString());
                        builder.setLength(0);
                        continue;
                    }
                    throw new IllegalArgumentException(
                            String.format("Could not parse WHERE conditions, unexpected %s: %s", COMMA, conditions));
                case EQUAL:
                    if (isQuoteContext(context)) {
                        builder.append(c);
                        continue;
                    }
                    tokens.addFirst(EQ);
                    continue;
                case NOT:
                    if (isQuoteContext(context)) {
                        builder.append(c);
                        continue;
                    }
                    next = conditionsWithTerminator.charAt(i + 1);
                    if (next == EQUAL) {
                        tokens.addFirst(NE_C);
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
                    next = conditionsWithTerminator.charAt(i + 1);
                    if (next == EQUAL) {
                        tokens.addFirst(GE);
                        i++;
                    } else {
                        tokens.addFirst(GT);
                    }
                    continue;
                case LESS:
                    if (isQuoteContext(context)) {
                        builder.append(c);
                        continue;
                    }
                    next = conditionsWithTerminator.charAt(i + 1);
                    if (next == EQUAL) {
                        tokens.addFirst(LE);
                        i++;
                    } else if (next == GREATER) {
                        tokens.addFirst(NE_ANSI);
                    } else {
                        tokens.addFirst(LT);
                    }
                    continue;
                case OPEN_PARENS:
                    if (isQuoteContext(context)) {
                        builder.append(c);
                        continue;
                    }
                    continue;
                case CLOSED_PARENS:
                    if (isQuoteContext(context)) {
                        builder.append(c);
                        continue;
                    }
                    if (parens.peekFirst() != OPEN_PARENS) {
                        throw new IllegalArgumentException(
                                String.format("Could not parse WHERE conditions, no matching open parens for the "
                                                + "closed parens: %s",
                                        conditions));
                    }
                    parens.removeFirst();
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

    private Object parse(final String token, final Context context) {
        final Optional<Operation> operation = isOperation(token, context);
        if (operation.isPresent()) {
            return operation.get();
        }
        final Optional<Operator> operator = isOperator(token, context);
        if (operator.isPresent()) {
            return operator.get();
        }
        return token;
    }

    private boolean isQuoteContext(final Context context) {
        return context == Context.SINGLE_QUOTE;
    }

    private boolean isSpecial(final Operation operation) {
        return operation == BETWEEN || operation == IN;
    }

    private void reduce(final Deque<Object> tokens) {
        final Deque<String> values = new LinkedList<>();
        while (!tokens.isEmpty() && !(tokens.peekFirst() instanceof Operation)) {
            values.addLast((String) tokens.removeFirst());
        }
        if (tokens.isEmpty()) {
            // put everything back
            values.forEach(tokens::addFirst);
            return;
        }
        // TODO: for each operation review whether everything is there, although for IN statement we could now,
        // so probably have to create more contexts
        // operation
        final Operation operation = (Operation) tokens.removeFirst();
        final String columnName = (String) tokens.removeFirst();
        final Expr expr = reduce(operation, columnName, values.toArray(new String[values.size()]));
        final Object action = tokens.peekFirst();
        if (action instanceof Operator) {
            tokens.addFirst(reduce((Operator) tokens.removeFirst(), (Expr) tokens.removeFirst(), expr));
        } else {
            tokens.addFirst(expr);
        }
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
            if (token.equals(operation.getSymbol())) {
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
        IN
    }
}
