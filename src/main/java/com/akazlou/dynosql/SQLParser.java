package com.akazlou.dynosql;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

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

    public Optional<SQLQuery> parse(final String query) {
        final Matcher matcher = SELECT_QUERY_PATTERN.matcher(query.trim());
        if (!matcher.matches()) {
            return Optional.empty();
        }
        final List<String> columns = parseColumns(matcher.group(COLUMNS_MATCHER_GROUP).split(COLUMNS_SEPARATOR));
        final String table = matcher.group(TABLE_MATCHER_GROUP).trim();
        final List<SQLQuery.Expr> conditions = parseConditions(matcher.group(CONDITIONS_MATCHER_GROUP));

        return Optional.of(new SQLQuery(table, columns, conditions));
    }

    private List<String> parseColumns(final String[] columns) {
        return Arrays.stream(columns)
                .map(String::trim)
                .map(column -> {
                    final String columnLower = column.toLowerCase(Locale.ROOT);
                    if (columnLower.contains(AS_KEYWORD)) {
                        return column.substring(columnLower.indexOf(AS_KEYWORD) + AS_KEYWORD.length())
                                .trim();
                    }
                    return column;
                })
                .collect(Collectors.toList());
    }

    private List<SQLQuery.Expr> parseConditions(final String conditions) {
        return new ArrayList<>();
    }
}
