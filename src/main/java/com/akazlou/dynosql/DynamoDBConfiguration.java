package com.akazlou.dynosql;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;

class DynamoDBConfiguration {
    private final DynamoDBEnvironment dynamoDBEnvironment;
    private final AWSCredentialsProvider awsCredentialsProvider;
    private final DynamoDB dynamoDB;

    DynamoDBConfiguration(final DynamoDBEnvironment dynamoDBEnvironment,
                          final AWSCredentialsProvider awsCredentialsProvider) {
        this.dynamoDBEnvironment = dynamoDBEnvironment;
        this.awsCredentialsProvider = awsCredentialsProvider;
        this.dynamoDB = createClient();
    }

    private DynamoDB createClient() {
        final AmazonDynamoDB amazonDynamoDB = AmazonDynamoDBClient.builder()
                .withCredentials(awsCredentialsProvider)
                .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(
                        dynamoDBEnvironment.getEndpoint(), dynamoDBEnvironment.getRegion().getName()))
                .build();
        return new DynamoDB(amazonDynamoDB);
    }
}
