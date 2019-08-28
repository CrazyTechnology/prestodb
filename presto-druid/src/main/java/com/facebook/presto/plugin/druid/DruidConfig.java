package com.facebook.presto.plugin.druid;

import io.airlift.configuration.Config;

public class DruidConfig {
    private String sqlTimeZone = "UTC";
    private String brokerUrl;
    private String coordinatorUrl;
    private String schema="druid";
    public String getSqlTimeZone()
    {
        return this.sqlTimeZone;
    }

    @Config("druid.sql-time-zone")
    public DruidConfig setSqlTimeZone(String sqlTimeZone) {
        this.sqlTimeZone = sqlTimeZone;
        return this;
    }
    public String getBrokerUrl() {
        return brokerUrl;
    }

    @Config("druid.broker-url")
    public DruidConfig setBrokerUrl(final String brokerUrl) {
        this.brokerUrl = brokerUrl;
        return this;
    }

    @Config("druid.schema")
    public DruidConfig setSchema(final String schema) {
        this.schema = schema;
        return this;
    }

    public String getSchema() {
        return this.schema;
    }

    public String getCoordinatorUrl() {
        return coordinatorUrl;
    }

    @Config("druid-coordinator-url")
    public DruidConfig setCoordinatorUrl(final String coordinatorUrl) {
        this.coordinatorUrl = coordinatorUrl;
        return this;
    }
}
