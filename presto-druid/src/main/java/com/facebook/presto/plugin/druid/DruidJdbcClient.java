package com.facebook.presto.plugin.druid;
import com.facebook.presto.plugin.jdbc.*;
import com.facebook.presto.spi.SchemaTableName;
import com.google.common.collect.ImmutableList;
import com.facebook.presto.spi.PrestoException;
import org.apache.calcite.avatica.remote.Driver;
import javax.inject.Inject;
import java.sql.*;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import static com.facebook.presto.plugin.jdbc.JdbcErrorCode.JDBC_ERROR;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static java.util.Locale.ENGLISH;


public class DruidJdbcClient extends BaseJdbcClient {
    @Inject
    public DruidJdbcClient(
            JdbcConnectorId connectorId,
            BaseJdbcConfig config,
            DruidConfig druidConfig) throws SQLException {
        super(connectorId, config, "", connectionFactory(config, druidConfig));
    }

    private static ConnectionFactory connectionFactory(
            BaseJdbcConfig config,
            DruidConfig druidConfig) throws SQLException {
//        Properties connectionProperties = basicConnectionProperties(config);
        //connectionProperties.setProperty("schema",druidConfig.getSchema());
        Properties connectionProperties=new Properties();
        return new DriverConnectionFactory(
                new Driver(),
                config.getConnectionUrl(),
                connectionProperties);
    }

    @Override
    public void abortReadConnection(Connection connection)
            throws SQLException
    {
        connection.abort(directExecutor());
    }



    protected ResultSet getTables(Connection connection,  Optional<String> schemaName, Optional<String> tableName)
            throws SQLException
    {
        DatabaseMetaData metadata = connection.getMetaData();
        Optional<String> escape = Optional.ofNullable(metadata.getSearchStringEscape());
        return metadata.getTables(
                connection.getCatalog(),
                escapeNamePattern(schemaName, escape).orElse(null),
                escapeNamePattern(tableName, escape).orElse(null),
                new String[] {"TABLE", "VIEW"});
    }

    @Override
    public List<SchemaTableName> getTableNames(JdbcIdentity identity, Optional<String> schema)
    {
        try (Connection connection = connectionFactory.openConnection(identity);) {
            try (ResultSet resultSet = getTables(connection, schema, null)) {
                ImmutableList.Builder<SchemaTableName> list = ImmutableList.builder();
                while (resultSet.next()) {
                    list.add(getSchemaTableName(resultSet));
                }
                return list.build();
            }
        }
        catch (SQLException e) {
            throw new PrestoException(JDBC_ERROR, e);
        }
        }

    protected SchemaTableName getSchemaTableName(ResultSet resultSet)
            throws SQLException
    {
        return new SchemaTableName(
                resultSet.getString("TABLE_SCHEM").toLowerCase(ENGLISH),
                resultSet.getString("TABLE_NAME").toLowerCase(ENGLISH));
    }





}