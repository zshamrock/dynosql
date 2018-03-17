package com.akazlou.dynosql;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
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

    public Optional<SQLQuery> parse(final String query) {
        final Matcher matcher = SELECT_QUERY_PATTERN.matcher(query.trim());
        if (!matcher.matches()) {
            return Optional.empty();
        }
        final String[] columns = matcher.group("columns").split(",");
        final String table = matcher.group("table").trim();
        final List<SQLQuery.Expr> conditions = parseConditions(matcher.group("conditions"));

        return Optional.of(new SQLQuery(
                table,
                Arrays.stream(columns).map(String::trim).collect(Collectors.toList()),
                conditions));
    }

    private List<SQLQuery.Expr> parseConditions(final String conditions) {
        return new ArrayList<>();
    }
}
