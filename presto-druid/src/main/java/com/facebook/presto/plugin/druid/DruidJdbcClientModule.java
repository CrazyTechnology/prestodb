package com.facebook.presto.plugin.druid;

import com.facebook.presto.plugin.jdbc.BaseJdbcConfig;
import com.facebook.presto.plugin.jdbc.ConnectionFactory;
import com.facebook.presto.plugin.jdbc.DriverConnectionFactory;
import com.facebook.presto.plugin.jdbc.JdbcClient;
import com.google.inject.Binder;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import org.apache.calcite.avatica.remote.Driver;

import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.util.Optional;
import java.util.Properties;

import static com.facebook.presto.plugin.jdbc.DriverConnectionFactory.basicConnectionProperties;
import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.configuration.ConfigBinder.configBinder;

public class DruidJdbcClientModule extends AbstractConfigurationAwareModule {
    @Override
    protected void setup(Binder binder) {
        binder.bind(JdbcClient.class).to(DruidJdbcClient.class).in(Scopes.SINGLETON);
        ensureCatalogIsEmpty(buildConfigObject(BaseJdbcConfig.class).getConnectionUrl());
        configBinder(binder).bindConfig(DruidConfig.class);
    }

    private static void ensureCatalogIsEmpty(String connectionUrl) {
        try {
            Driver driver = new Driver();
            DriverPropertyInfo[] driverPropertyInfo = driver.getPropertyInfo(connectionUrl, new Properties());
            checkArgument(connectionUrl != null, "Invalid JDBC URL for Druid connector");
        }
        catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }


}