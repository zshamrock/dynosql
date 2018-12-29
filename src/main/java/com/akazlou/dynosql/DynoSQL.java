package com.akazlou.dynosql;

import java.util.Collections;
import java.util.List;

import com.amazonaws.annotation.ThreadSafe;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.regions.Region;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;

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
    private final DynamoDB dynamoDB;

    public DynoSQL(final Region region) {
        dynamoDB = new DynamoDBConfiguration(new DynamoDBEnvironment(region), new DefaultAWSCredentialsProviderChain())
                .createClient();
    }

    public void close() {
        dynamoDB.shutdown();
    }

    public List<Item> query(final String sql) {
        // TODO: Implement
        return Collections.emptyList();
    }
}
