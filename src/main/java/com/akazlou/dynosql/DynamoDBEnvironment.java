package com.akazlou.dynosql;

import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;

class DynamoDBEnvironment {
    public String getEndpoint() {
        return "";
    }

    public Region getRegion() {
        return Regions.getCurrentRegion();
    }
}
