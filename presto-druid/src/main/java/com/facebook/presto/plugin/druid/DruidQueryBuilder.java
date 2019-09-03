package com.facebook.presto.plugin.druid;

import com.facebook.presto.plugin.jdbc.JdbcClient;
import com.facebook.presto.plugin.jdbc.JdbcColumnHandle;
import com.facebook.presto.plugin.jdbc.JdbcTypeHandle;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.predicate.Domain;
import com.facebook.presto.spi.predicate.Range;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.type.*;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import org.joda.time.DateTimeZone;

import java.sql.*;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Optional;

import static com.facebook.presto.spi.type.DateTimeEncoding.unpackMillisUtc;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Strings.isNullOrEmpty;
import static java.lang.Float.intBitsToFloat;
import static java.lang.String.format;
import static java.util.Collections.nCopies;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.DAYS;
import static java.util.stream.Collectors.joining;
import static org.joda.time.DateTimeZone.UTC;
import static org.weakref.jmx.internal.guava.collect.Iterables.getOnlyElement;

public class DruidQueryBuilder {
    // not all databases support booleans, so use 1=1 and 1=0 instead
    private static final String ALWAYS_TRUE = "1=1";
    private static final String ALWAYS_FALSE = "1=0";
    private static final Logger LOG = Logger.get(DruidQueryBuilder.class);

    private final String quote;

    private static class TypeAndValue {
        private final Type type;
        private final JdbcTypeHandle typeHandle;
        private final Object value;

        public TypeAndValue(Type type, JdbcTypeHandle typeHandle, Object value) {
            this.type = requireNonNull(type, "type is null");
            this.typeHandle = requireNonNull(typeHandle, "typeHandle is null");
            this.value = requireNonNull(value, "value is null");
        }

        public Type getType() {
            return type;
        }

        public JdbcTypeHandle getTypeHandle() {
            return typeHandle;
        }

        public Object getValue() {
            return value;
        }
    }

    public DruidQueryBuilder(String quote) {
        this.quote = requireNonNull(quote, "quote is null");
    }

    public PreparedStatement buildSql(
            JdbcClient client,
            Connection connection,
            String catalog,
            String schema,
            String table,
            List<JdbcColumnHandle> columns,
            TupleDomain<ColumnHandle> tupleDomain,
            Optional<String> additionalPredicate) throws SQLException {
        StringBuilder sql = new StringBuilder();

        String columnNames = columns.stream()
                .map(JdbcColumnHandle::getColumnName)
                .map(this::quote)
                .collect(joining(", "));

        sql.append("SELECT ");
        sql.append(columnNames);
        if (columns.isEmpty()) {
            sql.append("1"); // TODO: Select null does not work in druid-sql
        }

        sql.append(" FROM ");

        // if (!isNullOrEmpty(catalog)) {
        //   sql.append(quote(catalog)).append('.');
        // }

        if (!isNullOrEmpty(schema)) {
            sql.append(quote(schema)).append('.');
        }
        sql.append(quote(table));

        List<TypeAndValue> accumulator = new ArrayList<>();
        List<String> clauses = toConjuncts(client, columns, tupleDomain, accumulator);
        if (additionalPredicate.isPresent()) {
            clauses = ImmutableList.<String>builder()
                    .addAll(clauses)
                    .add(additionalPredicate.get())
                    .build();
        }
        if (!clauses.isEmpty()) {
            sql.append(" WHERE ")
                    .append(Joiner.on(" AND ").join(clauses));
        }

        String sqlPrepared = sql.toString();
        /* DRUID connector fails if values are not assigned. So we are going to build the SQL with values assigned */
        // TODO : cleanup this class once fixed https://issues.apache.org/jira/browse/CALCITE-2873
        for (int i = 0; i < accumulator.size(); i++) {
            sqlPrepared = assignVal(sqlPrepared, getStringValue(accumulator.get(i)));
        }
        LOG.info("Start Query SQL " + sqlPrepared);

        return client.getPreparedStatement(connection, sqlPrepared);
    }

    private String assignVal(String sql, String val) {
        return sql.replaceFirst("\\?", "'" + val + "'");
    }

    private String getStringValue(TypeAndValue typeAndValue) {
        if (typeAndValue.getType().equals(BigintType.BIGINT)) {
            return String.valueOf((long) typeAndValue.getValue());
        } else if (typeAndValue.getType().equals(IntegerType.INTEGER)) {
            return String.valueOf((((Number) typeAndValue.getValue()).intValue()));
        } else if (typeAndValue.getType().equals(SmallintType.SMALLINT)) {
            return String.valueOf(((Number) typeAndValue.getValue()).shortValue());
        } else if (typeAndValue.getType().equals(TinyintType.TINYINT)) {
            return String.valueOf(((Number) typeAndValue.getValue()).byteValue());
        } else if (typeAndValue.getType().equals(DoubleType.DOUBLE)) {
            return String.valueOf((double) typeAndValue.getValue());
        } else if (typeAndValue.getType().equals(RealType.REAL)) {
            return String.valueOf(intBitsToFloat(((Number) typeAndValue.getValue()).intValue()));
        } else if (typeAndValue.getType().equals(BooleanType.BOOLEAN)) {
            return String.valueOf((boolean) typeAndValue.getValue());
        } else if (typeAndValue.getType().equals(DateType.DATE)) {
            long millis = DAYS.toMillis((long) typeAndValue.getValue());
            return String.valueOf(new Date(UTC.getMillisKeepLocal(DateTimeZone.getDefault(), millis)));
        } else if (typeAndValue.getType().equals(TimeType.TIME)) {
            return String.valueOf(new Time((long) typeAndValue.getValue()));
        } else if (typeAndValue.getType().equals(TimeWithTimeZoneType.TIME_WITH_TIME_ZONE)) {
            return String.valueOf(new Time(unpackMillisUtc((long) typeAndValue.getValue())));
        } else if (typeAndValue.getType().equals(TimestampType.TIMESTAMP)) {
            return String.valueOf(new Timestamp((long) typeAndValue.getValue()));
        } else if (typeAndValue.getType().equals(TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE)) {
            return String.valueOf(new Timestamp(unpackMillisUtc((long) typeAndValue.getValue())));
        } else if (typeAndValue.getType() instanceof VarcharType) {
            return String.valueOf(((Slice) typeAndValue.getValue()).toStringUtf8());
        } else if (typeAndValue.getType() instanceof CharType) {
            return String.valueOf(((Slice) typeAndValue.getValue()).toStringUtf8());
        } else {
            throw new UnsupportedOperationException("Can't handle type: " + typeAndValue.getType());
        }
    }

//  private static Domain pushDownDomain(JdbcClient client, ConnectorSession session, JdbcColumnHandle column, Domain domain) {
//    return client.toPrestoType(session, column.getJdbcTypeHandle())
//      .orElseThrow(() -> new IllegalStateException(format("Unsupported type %s with handle %s", column.getColumnType(), column.getJdbcTypeHandle())))
//      .getPushdownConverter().apply(domain);
//  }

    private List<String> toConjuncts(JdbcClient client, List<JdbcColumnHandle> columns, TupleDomain<ColumnHandle> tupleDomain, List<TypeAndValue> accumulator) {
        ImmutableList.Builder<String> builder = ImmutableList.builder();
        for (JdbcColumnHandle column : columns) {
            if (tupleDomain.getDomains()==null) {
                continue;
            }

            Domain domain = tupleDomain.getDomains().get().get(column);
            if (domain != null) {
//        domain = pushDownDomain(client, session, column, domain);
                builder.add(toPredicate(column.getColumnName(), domain, column, accumulator));
            }
        }
        return builder.build();
    }

    private String toPredicate(String columnName, Domain domain, JdbcColumnHandle column, List<TypeAndValue> accumulator) {
        checkArgument(domain.getType().isOrderable(), "Domain type must be orderable");

        if (domain.getValues().isNone()) {
            return domain.isNullAllowed() ? quote(columnName) + " IS NULL" : ALWAYS_FALSE;
        }

        if (domain.getValues().isAll()) {
            return domain.isNullAllowed() ? ALWAYS_TRUE : quote(columnName) + " IS NOT NULL";
        }

        List<String> disjuncts = new ArrayList<>();
        List<Object> singleValues = new ArrayList<>();
        for (Range range : domain.getValues().getRanges().getOrderedRanges()) {
            checkState(!range.isAll()); // Already checked
            if (range.isSingleValue()) {
                singleValues.add(range.getLow().getValue());
            } else {
                List<String> rangeConjuncts = new ArrayList<>();
                if (!range.getLow().isLowerUnbounded()) {
                    switch (range.getLow().getBound()) {
                        case ABOVE:
                            rangeConjuncts.add(toPredicate(columnName, ">", range.getLow().getValue(), column, accumulator));
                            break;
                        case EXACTLY:
                            rangeConjuncts.add(toPredicate(columnName, ">=", range.getLow().getValue(), column, accumulator));
                            break;
                        case BELOW:
                            throw new IllegalArgumentException("Low marker should never use BELOW bound");
                        default:
                            throw new AssertionError("Unhandled bound: " + range.getLow().getBound());
                    }
                }
                if (!range.getHigh().isUpperUnbounded()) {
                    switch (range.getHigh().getBound()) {
                        case ABOVE:
                            throw new IllegalArgumentException("High marker should never use ABOVE bound");
                        case EXACTLY:
                            rangeConjuncts.add(toPredicate(columnName, "<=", range.getHigh().getValue(), column, accumulator));
                            break;
                        case BELOW:
                            rangeConjuncts.add(toPredicate(columnName, "<", range.getHigh().getValue(), column, accumulator));
                            break;
                        default:
                            throw new AssertionError("Unhandled bound: " + range.getHigh().getBound());
                    }
                }
                // If rangeConjuncts is null, then the range was ALL, which should already have been checked for
                checkState(!rangeConjuncts.isEmpty());
                disjuncts.add("(" + Joiner.on(" AND ").join(rangeConjuncts) + ")");
            }
        }

        // Add back all of the possible single values either as an equality or an IN predicate
        if (singleValues.size() == 1) {
            disjuncts.add(toPredicate(columnName, "=", getOnlyElement(singleValues), column, accumulator));
        } else if (singleValues.size() > 1) {
            for (Object value : singleValues) {
                bindValue(value, column, accumulator);
            }
            String values = Joiner.on(",").join(nCopies(singleValues.size(), "?"));
            disjuncts.add(quote(columnName) + " IN (" + values + ")");
        }

        // Add nullability disjuncts
        checkState(!disjuncts.isEmpty());
        if (domain.isNullAllowed()) {
            disjuncts.add(quote(columnName) + " IS NULL");
        }

        return "(" + Joiner.on(" OR ").join(disjuncts) + ")";
    }

    private String toPredicate(String columnName, String operator, Object value, JdbcColumnHandle column, List<TypeAndValue> accumulator) {
        bindValue(value, column, accumulator);
        return quote(columnName) + " " + operator + " ?";
    }

    private String quote(String name) {
        String quotedName = name.replace(quote, quote + quote);
        return quote + quotedName + quote;
    }

    private static void bindValue(Object value, JdbcColumnHandle column, List<TypeAndValue> accumulator) {
        Type type = column.getColumnType();
        accumulator.add(new TypeAndValue(type, column.getJdbcTypeHandle(), value));
    }
}
