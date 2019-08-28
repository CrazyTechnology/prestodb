package com.facebook.presto.plugin.druid;

import com.facebook.presto.plugin.jdbc.JdbcPlugin;

public class DruidJdbcPlugin extends JdbcPlugin {
    public DruidJdbcPlugin() {
        super("druid", new DruidJdbcClientModule());
    }
}
