package com.facebook.presto.plugin.druid;

import com.facebook.presto.plugin.jdbc.*;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.TableNotFoundException;
import com.facebook.presto.spi.type.VarcharType;
import com.google.common.base.CharMatcher;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.avatica.remote.Driver;

import javax.inject.Inject;
import java.sql.*;
import java.util.*;

import static com.facebook.presto.plugin.jdbc.DriverConnectionFactory.basicConnectionProperties;
import static com.facebook.presto.plugin.jdbc.JdbcErrorCode.JDBC_ERROR;
import static com.facebook.presto.plugin.jdbc.StandardReadMappings.varcharReadMapping;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.facebook.presto.spi.type.VarcharType.createUnboundedVarcharType;
import static com.facebook.presto.spi.type.VarcharType.createVarcharType;
import static com.google.common.collect.Iterables.getOnlyElement;
import static java.sql.ResultSetMetaData.columnNoNulls;


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
        Properties connectionProperties = basicConnectionProperties(config);
        return new DriverConnectionFactory(
                new Driver(),
                config.getConnectionUrl(),
                connectionProperties);
    }

//    @Override
//    public void abortReadConnection(Connection connection)
//            throws SQLException
//    {
//        connection.abort(directExecutor());
//    }

    @Override
    public PreparedStatement getPreparedStatement(Connection connection, String sql)
            throws SQLException {
        PreparedStatement statement = connection.prepareStatement(sql);
        return statement;
    }


    @Override
    protected ResultSet getTables(Connection connection, Optional<String> schemaName, Optional<String> tableName)
            throws SQLException {
        DatabaseMetaData metadata = connection.getMetaData();
        // For druid - catalog is empty.
        return metadata.getTables(connection.getCatalog(),
                schemaName.orElse(null),
                tableName.orElse(null),
                null);
    }
    protected SchemaTableName getSchemaTableName(ResultSet resultSet) throws SQLException {
        // Druid table/schema is case sensitive.
        return new SchemaTableName(
                resultSet.getString("TABLE_SCHEM"),
                resultSet.getString("TABLE_NAME"));
    }

    @Override
    public List<SchemaTableName> getTableNames(JdbcIdentity identity, Optional<String> schema) {
        try (Connection connection = connectionFactory.openConnection(identity)) {
            try (ResultSet resultSet = getTables(connection, schema, Optional.empty())) {
                ImmutableList.Builder<SchemaTableName> list = ImmutableList.builder();
                while (resultSet.next()) {
                    list.add(getSchemaTableName(resultSet));
                }
                return list.build();
            }
        } catch (SQLException e) {
            throw new PrestoException(JDBC_ERROR, e);
        }
    }







    @Override
    public JdbcTableHandle getTableHandle(JdbcIdentity identity, SchemaTableName schemaTableName) {
        try (Connection connection = connectionFactory.openConnection(identity)) {
            String jdbcSchemaName = schemaTableName.getSchemaName();
            String jdbcTableName = schemaTableName.getTableName();
            try (ResultSet resultSet = getTables(connection, Optional.of(jdbcSchemaName), Optional.of(jdbcTableName))) {
                List<JdbcTableHandle> tableHandles = new ArrayList<>();
                while (resultSet.next()) {
                    tableHandles.add(new JdbcTableHandle(
                            connectorId,
                            schemaTableName,
                            "druid",
                            resultSet.getString("TABLE_SCHEM"),
                            resultSet.getString("TABLE_NAME")));
                }
                if (tableHandles.isEmpty()) {
                    return null;
                }
                if (tableHandles.size() > 1) {
                    throw new PrestoException(NOT_SUPPORTED, "Multiple tables matched: " + schemaTableName);
                }
                return getOnlyElement(tableHandles);
            }
        } catch (SQLException e) {
            throw new PrestoException(JDBC_ERROR, e);
        }
    }

    @Override
    public Optional<ReadMapping> toPrestoType(ConnectorSession session, JdbcTypeHandle typeHandle) {
        switch (typeHandle.getJdbcType()) {
            case Types.VARCHAR:
                return jdbcTypeToPrestoType(typeHandle);
            default:
                return super.toPrestoType(session, typeHandle);
        }
    }
    private static Optional<ReadMapping> jdbcTypeToPrestoType(JdbcTypeHandle type)
    {
        int columnSize = type.getColumnSize();
        if (columnSize > VarcharType.MAX_LENGTH || columnSize == -1) {
            return Optional.of(varcharReadMapping(createUnboundedVarcharType()));
        }
        return Optional.of(varcharReadMapping(createVarcharType(columnSize)));
    }


    @Override
    public List<JdbcColumnHandle> getColumns(ConnectorSession session, JdbcTableHandle tableHandle) {
        try (Connection connection = connectionFactory.openConnection(JdbcIdentity.from(session))) {
            // Overriding this method because of the way we retrieve columns in druid.
            try (ResultSet resultSet = _getColumns(tableHandle, connection.getMetaData())) {
                List<JdbcColumnHandle> columns = new ArrayList<>();
                while (resultSet.next()) {
                    JdbcTypeHandle typeHandle = new JdbcTypeHandle(
                            resultSet.getInt("DATA_TYPE"),
                            resultSet.getInt("COLUMN_SIZE"),
                            resultSet.getInt("DECIMAL_DIGITS"));
                    Optional<ReadMapping> columnMapping = toPrestoType(session,typeHandle);
                    // skip unsupported column types
                    if (columnMapping.isPresent()) {
                        String columnName = resultSet.getString("COLUMN_NAME");
                        boolean nullable = (resultSet.getInt("NULLABLE") != columnNoNulls);
                        columns.add(new JdbcColumnHandle(connectorId, columnName, typeHandle, columnMapping.get().getType(), nullable));
                    }
                }
                if (columns.isEmpty()) {
                    // In rare cases (e.g. PostgreSQL) a table might have no columns.
                    throw new TableNotFoundException(tableHandle.getSchemaTableName());
                }
                return ImmutableList.copyOf(columns);
            }
        } catch (SQLException e) {
            throw new PrestoException(JDBC_ERROR, e);
        }
    }

    private ResultSet _getColumns(JdbcTableHandle tableHandle, DatabaseMetaData metadata) throws SQLException {
        return metadata.getColumns(
                tableHandle.getCatalogName(),
                tableHandle.getSchemaName(),
                tableHandle.getTableName(),
                null);
    }


//    public PreparedStatement buildSql(ConnectorSession session, Connection connection, JdbcSplit split, JdbcTableHandle table, List<JdbcColumnHandle> columns)
//            throws SQLException {
//        return new DruidQueryBuilder(identifierQuote)
//                .buildSql(this,
//                        session,
//                        connection,
//                        table.getCatalogName(),
//                        table.getSchemaName(),
//                        table.getTableName(),
//                        columns,
//                        table.getConstraint(),
//                        split.getAdditionalPredicate());
//    }
@Override
public PreparedStatement buildSql(Connection connection, JdbcSplit split, List<JdbcColumnHandle> columnHandles)
        throws SQLException
{
    return new DruidQueryBuilder(identifierQuote).buildSql(
            this,
            connection,
            split.getCatalogName(),
            split.getSchemaName(),
            split.getTableName(),
            columnHandles,
            split.getTupleDomain(),
            split.getAdditionalPredicate());
}
}