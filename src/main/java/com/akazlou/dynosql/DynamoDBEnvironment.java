package com.akazlou.dynosql;

import com.amazonaws.regions.Region;

class DynamoDBEnvironment {
    private static final String ENDPOINT_FORMAT_STRING = "https://dynamodb.%s.amazonaws.com";

    private final Region region;

    DynamoDBEnvironment(final Region region) {
        this.region = region;
    }

    Region getRegion() {
        return region;
    }

    String getEndpoint() {
        return String.format(ENDPOINT_FORMAT_STRING, region.getName());
    }
}
