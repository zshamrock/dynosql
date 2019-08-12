package com.akazlou.dynosql;

import static java.util.stream.Collectors.toList;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import com.amazonaws.annotation.ThreadSafe;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.regions.Region;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.spec.QuerySpec;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.TableDescription;
import com.amazonaws.services.dynamodbv2.xspec.ExpressionSpecBuilder;

/**
 * Entry point for executing the SQL queries against DynamoDB.
 *
 * <p>Class is thread-safe. So it is common to share this single instance among consumers, like exposing this class as
 * Spring {@code @Bean}, for example.</p>
 *
 * <p>Although remember to close the instance, i.e. using {@link #close()} to release all acquired resources. If using
 * as a Spring {@code @Bean}, you can set up the corresponding {@code destroyMethod} in there, ex.:
 * {@code @Bean(destroyMethod = "close")}.</p>
 */
@ThreadSafe
public class DynoSQL {
    private static final Logger logger = Logger.getLogger(DynoSQL.class.getName());

    private final DynamoDB dynamoDB;
    private final SQLParser parser;

    public DynoSQL(final Region region) {
        dynamoDB = new DynamoDBConfiguration(new DynamoDBEnvironment(region), new DefaultAWSCredentialsProviderChain())
                .createClient();
        parser = new SQLParser();
    }

    /**
     * Closes and releases all acquired resources.
     */
    public void close() {
        dynamoDB.shutdown();
    }

    /**
     * Runs the corresponding {@code sql} query and returns the list of {@link Item}-s matching the query.
     */
    public List<Item> query(final String sql) {
        final Optional<SQLQuery> result = parser.parse(sql);
        if (!result.isPresent()) {
            logger.warning(String.format("Was not able to parse SQL query %s", sql));
            return Collections.emptyList();
        }
        final SQLQuery query = result.get();
        final Table table = dynamoDB.getTable(query.getTableName());
        final ExpressionSpecBuilder builder = new ExpressionSpecBuilder();
        // TODO: Cache, but should be concurrently safe, i.e. ConcurrentHashMap
        final TableDescription description = table.getDescription();
        final Map<String, List<AttributeDefinition>> definitions = description.getAttributeDefinitions().stream()
                .collect(Collectors.groupingBy(AttributeDefinition::getAttributeName));
        final List<KeySchemaElement> schema = description.getKeySchema();
        final String hashName = schema.get(0).getAttributeName();
        final Optional<SQLQuery.Expr> conditions = query.getConditions();
        if (!conditions.isPresent()) {
            logger.warning(String.format("No HASH key %s in the WHERE clause of the SQL query %s", hashName, query));
            return Collections.emptyList();
        }
        final SQLQuery.Expr expr = conditions.get();
        new QuerySpec().withExpressionSpec(builder.buildForQuery());
        // TODO: Implement
        return Collections.emptyList();
    }

    /**
     * Runs the corresponding {@code sql} query and returns the list of domain objects matching the query.
     */
    public <T> List<T> query(final String sql, final Function<Item, T> mapper) {
        final List<Item> items = query(sql);
        return items.stream()
                .map(mapper)
                .collect(Collectors.collectingAndThen(toList(), Collections::unmodifiableList));
    }
}
