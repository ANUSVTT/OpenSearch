/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import com.google.common.collect.ImmutableList;
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptSchema;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelDistributions;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelReferentialConstraint;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.ColumnStrategy;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlLibraryOperators;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeTransforms;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Optionality;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.analytics.planner.rel.OpenSearchStageInputScan;
import org.opensearch.analytics.spi.AggregateFunction;
import org.opensearch.analytics.spi.DelegatedPredicateFunction;
import org.opensearch.analytics.spi.DelegationPossibleFunction;
import org.opensearch.analytics.spi.FragmentConvertor;
import org.opensearch.be.datafusion.planner.adapter.NumericConversionFunctionAdapter;
import org.opensearch.be.datafusion.planner.adapter.TimeConversionFunctionAdapter;

import java.math.BigDecimal;
import java.math.MathContext;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;

import io.substrait.expression.AggregateFunctionInvocation;
import io.substrait.expression.Expression;
import io.substrait.expression.FunctionArg;
import io.substrait.expression.ImmutableAggregateFunctionInvocation;
import io.substrait.extension.ExtensionCollector;
import io.substrait.extension.SimpleExtension;
import io.substrait.isthmus.ConverterProvider;
import io.substrait.isthmus.SubstraitRelVisitor;
import io.substrait.isthmus.TypeConverter;
import io.substrait.isthmus.expression.AggregateFunctionConverter;
import io.substrait.isthmus.expression.FunctionMappings;
import io.substrait.isthmus.expression.ScalarFunctionConverter;
import io.substrait.isthmus.expression.WindowFunctionConverter;
import io.substrait.plan.Plan;
import io.substrait.plan.PlanProtoConverter;
import io.substrait.plan.ProtoPlanConverter;
import io.substrait.proto.PlanRel;
import io.substrait.proto.ReadRel;
import io.substrait.relation.Aggregate;
import io.substrait.relation.Fetch;
import io.substrait.relation.Filter;
import io.substrait.relation.Project;
import io.substrait.relation.Rel;
import io.substrait.relation.Sort;
import io.substrait.type.NamedStruct;
import io.substrait.type.Type;
import io.substrait.type.proto.TypeProtoConverter;

/** Converts Calcite RelNode fragments to Substrait protobuf bytes for the DataFusion Rust runtime. */
public class DataFusionFragmentConvertor implements FragmentConvertor {

    private static final Logger LOGGER = LogManager.getLogger(DataFusionFragmentConvertor.class);

    /** Per-field accessors for {@code pattern_parser}'s STRUCT output; see {@link ItemTypeRebuilder}. */
    static final SqlOperator LOCAL_PATTERN_PARSER_GET_PATTERN_OP = new SqlFunction(
        "pattern_parser_get_pattern",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.VARCHAR_FORCE_NULLABLE,
        null,
        OperandTypes.ANY_ANY,
        SqlFunctionCategory.USER_DEFINED_FUNCTION
    );

    static final SqlOperator LOCAL_PATTERN_PARSER_GET_TOKENS_OP = new SqlFunction(
        "pattern_parser_get_tokens",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.ARG0_NULLABLE,
        null,
        OperandTypes.ANY_ANY,
        SqlFunctionCategory.USER_DEFINED_FUNCTION
    );

    /**
     * POC nested (N1): Custom operator for accessing a struct field from a LIST&lt;STRUCT&gt; column.
     * Semantics: unnest_field(array_col, 'field_name') → explodes the array and extracts the named field.
     * Maps to "unnest_field" Substrait extension function; the Rust side will interpret this as
     * UNNEST + struct field access.
     */
    static final SqlOperator LOCAL_UNNEST_FIELD_OP = new SqlFunction(
        "unnest_field",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.VARCHAR_FORCE_NULLABLE,
        null,
        OperandTypes.ANY_ANY,
        SqlFunctionCategory.USER_DEFINED_FUNCTION
    );

    private static final List<FunctionMappings.Sig> ADDITIONAL_SCALAR_SIGS = List.of(
        FunctionMappings.s(DelegatedPredicateFunction.FUNCTION, DelegatedPredicateFunction.NAME),
        FunctionMappings.s(AggregateFunction.REDUCE_EVAL_OP, "reduce_eval"),
        FunctionMappings.s(DelegationPossibleFunction.FUNCTION, DelegationPossibleFunction.NAME),
        FunctionMappings.s(SqlStdOperatorTable.ASCII, "ascii"),
        FunctionMappings.s(SqlStdOperatorTable.CHAR_LENGTH, "length"),
        FunctionMappings.s(SqlLibraryOperators.CONCAT_FUNCTION, "concat"),
        FunctionMappings.s(SqlLibraryOperators.CONCAT_WS, "concat_ws"),
        FunctionMappings.s(SqlLibraryOperators.REGEXP_LIKE, "regexp_like"),
        FunctionMappings.s(SqlLibraryOperators.ILIKE, "ilike"),
        FunctionMappings.s(SqlLibraryOperators.DATE_PART, "date_part"),
        FunctionMappings.s(SqlLibraryOperators.TO_CHAR, "to_char"),
        FunctionMappings.s(IpBinaryCastFunctionAdapter.IP_TO_STRING_OP, "ip_to_string"),
        FunctionMappings.s(IpBinaryCastFunctionAdapter.BINARY_TO_BASE64_OP, "binary_to_base64"),
        FunctionMappings.s(SqlLibraryOperators.DATE_TRUNC, "date_trunc"),
        FunctionMappings.s(SpanAdapter.LOCAL_DATE_BIN_OP, "date_bin"),
        FunctionMappings.s(PatternParserAdapter.LOCAL_PATTERN_PARSER_OP, "pattern_parser"),
        FunctionMappings.s(LOCAL_PATTERN_PARSER_GET_PATTERN_OP, "pattern_parser_get_pattern"),
        FunctionMappings.s(LOCAL_PATTERN_PARSER_GET_TOKENS_OP, "pattern_parser_get_tokens"),
        FunctionMappings.s(LOCAL_UNNEST_FIELD_OP, "unnest_field"),
        FunctionMappings.s(ConvertTzAdapter.LOCAL_CONVERT_TZ_OP, "convert_tz"),
        FunctionMappings.s(ParseAdapter.LOCAL_PARSE_OP, "parse"),
        FunctionMappings.s(GrokAdapter.LOCAL_GROK_OP, "grok"),
        FunctionMappings.s(SqlStdOperatorTable.ITEM, "item"),
        FunctionMappings.s(UnixTimestampAdapter.LOCAL_TO_UNIXTIME_OP, "to_unixtime"),
        FunctionMappings.s(DateTimeAdapters.LOCAL_NOW_OP, "now"),
        FunctionMappings.s(DateTimeAdapters.LOCAL_CURRENT_DATE_OP, "current_date"),
        FunctionMappings.s(DateTimeAdapters.LOCAL_CURRENT_TIME_OP, "current_time"),
        FunctionMappings.s(DateTimeAdapters.LOCAL_TIME_OP, "to_time"),
        FunctionMappings.s(DateTimeAdapters.LOCAL_DATE_OP, "to_date"),
        FunctionMappings.s(DateTimeAdapters.LOCAL_TO_TIMESTAMP_OP, "to_timestamp"),
        FunctionMappings.s(DateTimeAdapters.LOCAL_DATE_TRUNC_OP, "date_trunc"),
        FunctionMappings.s(RustUdfDateTimeAdapters.LOCAL_EXTRACT_OP, "opensearch_extract"),
        FunctionMappings.s(RustUdfDateTimeAdapters.LOCAL_FROM_UNIXTIME_OP, "from_unixtime"),
        FunctionMappings.s(RustUdfDateTimeAdapters.LOCAL_MAKEDATE_OP, "makedate"),
        FunctionMappings.s(RustUdfDateTimeAdapters.LOCAL_MAKETIME_OP, "maketime"),
        FunctionMappings.s(RustUdfDateTimeAdapters.LOCAL_DATE_FORMAT_OP, "date_format"),
        FunctionMappings.s(RustUdfDateTimeAdapters.LOCAL_TIME_FORMAT_OP, "time_format"),
        FunctionMappings.s(RustUdfDateTimeAdapters.LOCAL_STR_TO_DATE_OP, "str_to_date"),
        FunctionMappings.s(RustUdfDateTimeAdapters.LOCAL_OS_WEEK_OP, "os_week"),
        FunctionMappings.s(RustUdfDateTimeAdapters.LOCAL_OS_YEARWEEK_OP, "os_yearweek"),
        FunctionMappings.s(SqlLibraryOperators.REGEXP_CONTAINS, "regex_match"),
        FunctionMappings.s(SqlStdOperatorTable.REPLACE, "replace"),
        FunctionMappings.s(SqlLibraryOperators.REGEXP_REPLACE_3, "regexp_replace"),
        FunctionMappings.s(SqlLibraryOperators.REGEXP_REPLACE_PG_4, "regexp_replace"),
        FunctionMappings.s(SqlLibraryOperators.REVERSE, "reverse"),
        FunctionMappings.s(SqlLibraryOperators.TRANSLATE3, "translate"),
        FunctionMappings.s(PositionAdapter.STRPOS, "strpos"),
        FunctionMappings.s(StrftimeFunctionAdapter.STRFTIME, "strftime"),
        FunctionMappings.s(ToNumberFunctionAdapter.TONUMBER, "tonumber"),
        FunctionMappings.s(ToStringFunctionAdapter.TOSTRING, "tostring"),
        FunctionMappings.s(SqlLibraryOperators.MD5, "md5"),
        FunctionMappings.s(SqlLibraryOperators.SHA1, "sha1"),
        FunctionMappings.s(SqlLibraryOperators.CRC32, "crc32"),
        FunctionMappings.s(Sha2FunctionAdapter.DIGEST, "digest"),
        FunctionMappings.s(Sha2FunctionAdapter.ENCODE, "encode"),
        FunctionMappings.s(RexExtractAdapter.LOCAL_REX_EXTRACT_OP, "rex_extract"),
        FunctionMappings.s(RexExtractMultiAdapter.LOCAL_REX_EXTRACT_MULTI_OP, "rex_extract_multi"),
        FunctionMappings.s(RexOffsetAdapter.LOCAL_REX_OFFSET_OP, "rex_offset"),
        FunctionMappings.s(SqlLibraryOperators.ARRAY_LENGTH, "array_length"),
        FunctionMappings.s(NumericConversionFunctionAdapter.NUM, "num"),
        FunctionMappings.s(NumericConversionFunctionAdapter.AUTO, "auto"),
        FunctionMappings.s(NumericConversionFunctionAdapter.MEMK, "memk"),
        FunctionMappings.s(NumericConversionFunctionAdapter.RMCOMMA, "rmcomma"),
        FunctionMappings.s(NumericConversionFunctionAdapter.RMUNIT, "rmunit"),
        FunctionMappings.s(NumericConversionFunctionAdapter.DUR2SEC, "dur2sec"),
        FunctionMappings.s(NumericConversionFunctionAdapter.MSTIME, "mstime"),
        FunctionMappings.s(TimeConversionFunctionAdapter.CTIME, "ctime"),
        FunctionMappings.s(TimeConversionFunctionAdapter.MKTIME, "mktime"),
        FunctionMappings.s(SqlStdOperatorTable.TRUNCATE, "trunc"),
        FunctionMappings.s(SqlStdOperatorTable.CBRT, "cbrt"),
        FunctionMappings.s(SqlStdOperatorTable.COT, "cot"),
        FunctionMappings.s(SqlStdOperatorTable.PI, "pi"),
        FunctionMappings.s(SqlStdOperatorTable.RAND, "random"),
        FunctionMappings.s(SqlLibraryOperators.LOG, "logb"),
        FunctionMappings.s(SignumFunction.FUNCTION, SignumFunction.NAME),
        FunctionMappings.s(JsonFunctionAdapters.JsonAdapter.LOCAL_JSON_OP, "json"),
        FunctionMappings.s(JsonFunctionAdapters.JsonAppendAdapter.LOCAL_JSON_APPEND_OP, "json_append"),
        FunctionMappings.s(JsonFunctionAdapters.JsonArrayAdapter.LOCAL_JSON_ARRAY_OP, "json_array"),
        FunctionMappings.s(JsonFunctionAdapters.JsonArrayLengthAdapter.LOCAL_JSON_ARRAY_LENGTH_OP, "json_array_length"),
        FunctionMappings.s(JsonFunctionAdapters.JsonDeleteAdapter.LOCAL_JSON_DELETE_OP, "json_delete"),
        FunctionMappings.s(JsonFunctionAdapters.JsonExtendAdapter.LOCAL_JSON_EXTEND_OP, "json_extend"),
        FunctionMappings.s(JsonFunctionAdapters.JsonExtractAdapter.LOCAL_JSON_EXTRACT_OP, "json_extract"),
        FunctionMappings.s(JsonFunctionAdapters.JsonExtractAllAdapter.LOCAL_JSON_EXTRACT_ALL_OP, "json_extract_all"),
        FunctionMappings.s(JsonFunctionAdapters.JsonKeysAdapter.LOCAL_JSON_KEYS_OP, "json_keys"),
        FunctionMappings.s(JsonFunctionAdapters.JsonObjectAdapter.LOCAL_JSON_OBJECT_OP, "json_object"),
        FunctionMappings.s(JsonFunctionAdapters.JsonSetAdapter.LOCAL_JSON_SET_OP, "json_set"),
        FunctionMappings.s(JsonFunctionAdapters.JsonValidAdapter.LOCAL_JSON_VALID_OP, "json_valid"),
        FunctionMappings.s(SqlLibraryOperators.REGEXP_CONTAINS, "regex_match"),
        FunctionMappings.s(SqlStdOperatorTable.REPLACE, "replace"),
        FunctionMappings.s(SqlLibraryOperators.REGEXP_REPLACE_3, "regexp_replace"),
        FunctionMappings.s(SqlLibraryOperators.ARRAY_LENGTH, "array_length"),
        FunctionMappings.s(SqlLibraryOperators.ARRAY_SLICE, "array_slice"),
        FunctionMappings.s(SqlLibraryOperators.ARRAY_DISTINCT, "array_distinct"),
        FunctionMappings.s(MakeArrayAdapter.LOCAL_MAKE_ARRAY_OP, "make_array"),
        FunctionMappings.s(ArrayToStringAdapter.LOCAL_ARRAY_TO_STRING_OP, "array_to_string"),
        FunctionMappings.s(ArrayElementAdapter.LOCAL_ARRAY_ELEMENT_OP, "array_element"),
        FunctionMappings.s(ArrayElementAdapter.LOCAL_MAP_EXTRACT_OP, "map_extract"),
        FunctionMappings.s(MvzipAdapter.LOCAL_MVZIP_OP, "mvzip"),
        FunctionMappings.s(MvfindAdapter.LOCAL_MVFIND_OP, "mvfind"),
        FunctionMappings.s(MvappendAdapter.LOCAL_MVAPPEND_OP, "mvappend"),
        FunctionMappings.s(SpanBucketAdapter.LOCAL_SPAN_BUCKET_OP, "span_bucket"),
        FunctionMappings.s(WidthBucketAdapter.LOCAL_WIDTH_BUCKET_OP, "width_bucket"),
        FunctionMappings.s(MinspanBucketAdapter.LOCAL_MINSPAN_BUCKET_OP, "minspan_bucket"),
        FunctionMappings.s(RangeBucketAdapter.LOCAL_RANGE_BUCKET_OP, "range_bucket"),
        FunctionMappings.s(ConvAdapter.LOCAL_CONV_OP, "conv")
    );

    // TODO: extract these LOCAL_*_OP aggregate stubs (+ LocalAggOp and ADDITIONAL_AGGREGATE_SIGS)
    // into their own class, mirroring the per-function scalar *Adapter classes, to keep this file
    // from accumulating every aggregate definition. Pure structural move (no behaviour change);
    // updates the cross-file `DataFusionFragmentConvertor.LOCAL_*` references.
    /** Local stubs for PPL state-expanding aggregates; swapped in by {@link PplAggregateCallRewriter}. */
    static final SqlAggFunction LOCAL_TAKE_OP = new SqlAggFunction(
        "take",
        null,
        SqlKind.OTHER_FUNCTION,
        // FORCE_NULLABLE so AggregateCall.create accepts a nullable explicit return type.
        ReturnTypes.TO_ARRAY.andThen(SqlTypeTransforms.FORCE_NULLABLE),
        null,
        OperandTypes.VARIADIC,
        SqlFunctionCategory.USER_DEFINED_FUNCTION,
        false,
        false,
        Optionality.FORBIDDEN
    ) {
    };

    static final SqlAggFunction LOCAL_FIRST_OP = new SqlAggFunction(
        "first_value",
        null,
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.ARG0,
        null,
        OperandTypes.ANY,
        SqlFunctionCategory.USER_DEFINED_FUNCTION,
        false,
        false,
        Optionality.FORBIDDEN
    ) {
    };

    static final SqlAggFunction LOCAL_LAST_OP = new SqlAggFunction(
        "last_value",
        null,
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.ARG0,
        null,
        OperandTypes.ANY,
        SqlFunctionCategory.USER_DEFINED_FUNCTION,
        false,
        false,
        Optionality.FORBIDDEN
    ) {
    };

    /**
     * LIST/VALUES — carries the PPL element-rendering contract via the {@link LocalAggOp} hooks:
     * cast to VARCHAR (lowercase booleans), drop nulls, and (VALUES only) lexicographic sort.
     * Inferred return type is {@code ARRAY<VARCHAR>}.
     */
    static final SqlAggFunction LOCAL_ARRAY_AGG_OP = new LocalAggOp("array_agg", SqlKind.OTHER_FUNCTION, opBinding -> {
        RelDataTypeFactory tf = opBinding.getTypeFactory();
        return tf.createTypeWithNullability(tf.createArrayType(tf.createSqlType(SqlTypeName.VARCHAR), -1), true);
    }, OperandTypes.ANY) {
        @Override
        public Optional<RexNode> rewriteDataArg(int argIndex, RexNode argRef, RexBuilder rexBuilder, RelDataTypeFactory typeFactory) {
            // Skip array operands (partial→final merge path) and already-VARCHAR operands.
            if (argRef.getType().getComponentType() != null || argRef.getType().getSqlTypeName() == SqlTypeName.VARCHAR) {
                return Optional.empty();
            }
            return Optional.of(castToVarchar(argRef, rexBuilder, typeFactory));
        }

        @Override
        public boolean sortsArgAscending(AggregateCall call) {
            // VALUES (isDistinct) returns lexicographically sorted distinct strings; LIST does not sort.
            return call.isDistinct();
        }

        @Override
        public boolean filtersNullArgs(AggregateCall call) {
            // list/values drop null elements per the PPL contract.
            return true;
        }
    };

    /**
     * Casts a list/values element to VARCHAR matching the SQL plugin's {@code String.valueOf}
     * rendering: ip→{@code ip_to_string}, binary→{@code binary_to_base64}, else a plain CAST.
     * Unlike the {@code cast}/{@code tostring} path this does NOT uppercase booleans — native
     * {@code cast(boolean AS Utf8)} yields lowercase {@code true}/{@code false}, per the PPL
     * {@code list}/{@code values} contract.
     */
    private static RexNode castToVarchar(RexNode arg, RexBuilder rexBuilder, RelDataTypeFactory typeFactory) {
        RelDataType varcharNullable = typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.VARCHAR), true);
        if (arg.getType() instanceof org.opensearch.analytics.schema.IpType) {
            return rexBuilder.makeCall(varcharNullable, IpBinaryCastFunctionAdapter.IP_TO_STRING_OP, List.of(arg));
        }
        if (arg.getType() instanceof org.opensearch.analytics.schema.BinaryType) {
            return rexBuilder.makeCall(varcharNullable, IpBinaryCastFunctionAdapter.BINARY_TO_BASE64_OP, List.of(arg));
        }
        return rexBuilder.makeCast(varcharNullable, arg);
    }

    /** FINAL-side merge for LIST; un-nests per-shard list states. */
    static final SqlAggFunction LOCAL_LIST_MERGE_OP = new SqlAggFunction(
        "list_merge",
        null,
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.ARG0,
        null,
        OperandTypes.ANY,
        SqlFunctionCategory.USER_DEFINED_FUNCTION,
        false,
        false,
        Optionality.FORBIDDEN
    ) {
    };

    /** FINAL-side merge for VALUES — re-deduplicates after concatenation. */
    static final SqlAggFunction LOCAL_LIST_MERGE_DISTINCT_OP = new SqlAggFunction(
        "list_merge_distinct",
        null,
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.ARG0,
        null,
        OperandTypes.ANY,
        SqlFunctionCategory.USER_DEFINED_FUNCTION,
        false,
        false,
        Optionality.FORBIDDEN
    ) {
    };

    /**
     * PPL {@code percentile_approx(field, percentile)} → DataFusion's builtin
     * {@code approx_percentile_cont(field, percentile)}. PPL's trailing field-type-flag
     * arg is stripped by {@link PplAggregateCallRewriter} before binding; the percentile
     * literal is rescaled from PPL's [0, 100] to DataFusion's [0, 1] convention via
     * {@link LocalAggOp#normaliseLiteralArg} at substrait emission.
     */
    static final LocalAggOp LOCAL_PERCENTILE_APPROX_OP = new LocalAggOp(
        "approx_percentile_cont",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.ARG0.andThen(SqlTypeTransforms.FORCE_NULLABLE),
        OperandTypes.ANY_ANY
    ) {
        @Override
        public RexNode normaliseLiteralArg(int argIndex, RexLiteral lit, RexBuilder rexBuilder, RelDataTypeFactory typeFactory) {
            // The percentile literal arrives as INTEGER for the standard form percentile(x, 50)
            // but DOUBLE for the percNN/pNN shortcut (perc50 → 50.0E0), and getValue()'s backing
            // type differs between the two. Read it through getValueAs so both representations
            // rescale uniformly; pattern-matching get() on BigDecimal missed the DOUBLE shortcut
            // and let the unscaled 50.0 reach DataFusion ("must be between 0.0 and 1.0").
            if (argIndex == 1 && SqlTypeName.NUMERIC_TYPES.contains(lit.getType().getSqlTypeName())) {
                BigDecimal bd = lit.getValueAs(BigDecimal.class);
                if (bd != null) {
                    BigDecimal scaled = bd.divide(BigDecimal.valueOf(100), MathContext.DECIMAL64);
                    RelDataType doubleType = typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.DOUBLE), true);
                    return rexBuilder.makeLiteral(scaled, doubleType);
                }
                // bd == null only for a SQL-NULL percent (getValueAs returns null for NULL value) —
                // not a valid percentile and never produced by the percNN/pNN suffix or an explicit
                // numeric percentile() arg. Pass it through unchanged; DataFusion rejects a NULL
                // percentile at planning. There is no scaled-vs-unscaled ambiguity (NULL ≠ 50.0).
            }
            return lit;
        }
    };

    /** BRAIN window stub for {@code patterns ... method=BRAIN mode=label}. */
    static final SqlAggFunction LOCAL_INTERNAL_PATTERN_WINDOW_OP = new SqlAggFunction(
        "internal_pattern",
        null,
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.VARCHAR_FORCE_NULLABLE,
        null,
        OperandTypes.VARIADIC,
        SqlFunctionCategory.USER_DEFINED_FUNCTION,
        false,
        false,
        Optionality.FORBIDDEN
    ) {
    };

    /** BRAIN aggregate stub; return type is supplied by {@link PplAggregateCallRewriter}. */
    static final SqlAggFunction LOCAL_INTERNAL_PATTERN_OP = new SqlAggFunction(
        "internal_pattern",
        null,
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.ARG0,
        null,
        OperandTypes.VARIADIC,
        SqlFunctionCategory.USER_DEFINED_FUNCTION,
        false,
        false,
        Optionality.FORBIDDEN
    ) {
    };

    /**
     * Exact distinct count for use in a window context — replaces
     * {@code count(distinct x) OVER(...)} via {@link WindowFunctionAdapters#countDistinctExact()}.
     * Encoding DISTINCT in the operator name avoids the dropped-DISTINCT bug in DataFusion 54.x's
     * substrait window consumer. Custom Rust UDAF in {@code rust/src/udaf/os_count_distinct.rs}.
     */
    static final SqlAggFunction LOCAL_OS_COUNT_DISTINCT_OP = new SqlAggFunction(
        "os_count_distinct",
        null,
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.BIGINT,
        null,
        OperandTypes.ANY,
        SqlFunctionCategory.USER_DEFINED_FUNCTION,
        false,
        false,
        Optionality.FORBIDDEN
    ) {
    };

    private static final List<FunctionMappings.Sig> ADDITIONAL_AGGREGATE_SIGS = List.of(
        FunctionMappings.s(SqlStdOperatorTable.APPROX_COUNT_DISTINCT, "approx_distinct"),
        FunctionMappings.s(LOCAL_TAKE_OP, "take"),
        FunctionMappings.s(LOCAL_FIRST_OP, "first_value"),
        FunctionMappings.s(LOCAL_LAST_OP, "last_value"),
        FunctionMappings.s(LOCAL_ARRAY_AGG_OP, "array_agg"),
        FunctionMappings.s(LOCAL_LIST_MERGE_OP, "list_merge"),
        FunctionMappings.s(LOCAL_LIST_MERGE_DISTINCT_OP, "list_merge_distinct"),
        FunctionMappings.s(LOCAL_PERCENTILE_APPROX_OP, "approx_percentile_cont"),
        FunctionMappings.s(LOCAL_INTERNAL_PATTERN_OP, "internal_pattern"),
        FunctionMappings.s(LOCAL_OS_COUNT_DISTINCT_OP, "os_count_distinct")
    );

    private static final List<FunctionMappings.Sig> ADDITIONAL_WINDOW_SIGS = List.of(
        FunctionMappings.s(LOCAL_INTERNAL_PATTERN_WINDOW_OP, "internal_pattern"),
        // Mirror ADDITIONAL_AGGREGATE_SIGS: rename APPROX_COUNT_DISTINCT to DataFusion's `approx_distinct`.
        FunctionMappings.s(SqlStdOperatorTable.APPROX_COUNT_DISTINCT, "approx_distinct"),
        FunctionMappings.s(LOCAL_OS_COUNT_DISTINCT_OP, "os_count_distinct")
    );

    /**
     * Shared {@link TypeProtoConverter} for schema-only conversions. Safe as a singleton
     * because schema-only Reads convert primitive Calcite types to primitive Substrait
     * protos — no functions or user-defined types touch the inner {@link ExtensionCollector},
     * so it never accumulates per-call state. Avoids re-allocating both objects on every
     * {@link #convertSchemaOnlyRead} call.
     */
    private static final TypeProtoConverter SCHEMA_ONLY_TYPE_PROTO_CONVERTER = new TypeProtoConverter(new ExtensionCollector());

    private final SimpleExtension.ExtensionCollection extensions;

    public DataFusionFragmentConvertor(SimpleExtension.ExtensionCollection extensions) {
        this.extensions = extensions;
    }

    @Override
    public byte[] convertFragment(RelNode fragment) {
        LOGGER.debug("Converting fragment [{}]", fragment.getClass().getSimpleName());
        RelNode rewritten = rewriteStageInputScans(fragment);
        return convertToSubstrait(rewritten);
    }

    @Override
    public byte[] attachPartialAggOnTop(RelNode partialAggFragment, byte[] innerBytes) {
        LOGGER.debug("Attaching partial aggregate on top of {} inner bytes", innerBytes.length);
        Plan inner = decodePlan(innerBytes);
        Rel wrapper = convertStandalone(partialAggFragment);
        Plan rewired = rewire(
            inner,
            withAggregationPhase(wrapper, Expression.AggregationPhase.INITIAL_TO_INTERMEDIATE),
            fieldNames(partialAggFragment)
        );
        return serializePlan(SubstraitPlanPojoRewriter.rewrite(rewired));
    }

    /**
     * Builds a schema-only stub plan directly via Substrait protos — no isthmus, no
     * Calcite RelNode round-trip. Output:
     * <pre>
     *   Plan { relations: [PlanRel { Root { input: Rel { Read { named_table: "input-&lt;id&gt;";
     *                                                          base_schema: rowType } },
     *                                 names: rowType.fieldNames }}] }
     * </pre>
     *
     * <p>Used by the LM stage path: LM runs Java-only scatter/gather/stitch and emits no
     * Substrait compute, but the parent reduce sink (Stage 3) still calls
     * {@code registerPartitionStream} which needs the partition's named-table id and base
     * schema. This stub is the minimum proto that satisfies that path. Bypassing isthmus
     * avoids unnecessary {@code SubstraitRelVisitor} setup and keeps the produced bytes
     * tightly scoped to the schema we care about.
     */
    @Override
    public byte[] convertSchemaOnlyRead(int childStageId, RelDataType rowType) {
        // Fully-qualified names below: io.substrait.proto.{Plan,Rel,NamedStruct,RelRoot} clash with already-imported single-name imports.
        NamedStruct ns = TypeConverter.DEFAULT.toNamedStruct(rowType);
        io.substrait.proto.NamedStruct nsProto = ns.toProto(SCHEMA_ONLY_TYPE_PROTO_CONVERTER);

        ReadRel readRel = ReadRel.newBuilder()
            .setNamedTable(ReadRel.NamedTable.newBuilder().addNames("input-" + childStageId).build())
            .setBaseSchema(nsProto)
            .build();

        io.substrait.proto.Rel inputRel = io.substrait.proto.Rel.newBuilder().setRead(readRel).build();
        PlanRel planRel = PlanRel.newBuilder()
            .setRoot(io.substrait.proto.RelRoot.newBuilder().setInput(inputRel).addAllNames(rowType.getFieldNames()).build())
            .build();

        byte[] bytes = SubstraitPlanProtoRewriter.rewrite(io.substrait.proto.Plan.newBuilder().addRelations(planRel).build()).toByteArray();
        LOGGER.debug("Schema-only Read for stage [{}]: {} bytes", childStageId, bytes.length);
        return bytes;
    }

    @Override
    public byte[] attachFragmentOnTop(RelNode fragment, byte[] innerBytes) {
        LOGGER.debug("Attaching generic fragment [{}] on top of {} inner bytes", fragment.getClass().getSimpleName(), innerBytes.length);
        Plan inner = decodePlan(innerBytes);
        RelNode rewritten = rewriteStageInputScans(fragment);
        Rel wrapper = convertStandalone(rewritten);
        // Rewriter must run on the assembled plan so wrapper literals get rewritten alongside the inner.
        return serializePlan(SubstraitPlanPojoRewriter.rewrite(rewire(inner, wrapper, fieldNames(fragment))));
    }

    /**
     * Shared pre-Substrait rewrite pipeline. Both the top-level fragment path
     * ({@link #convertToSubstrait}) and the wrapper/partial-aggregate path
     * ({@link #convertStandalone}) must run the identical set of rewriters so a shape handled on
     * one path is not missed on the other. Centralizing here keeps the two paths in lockstep as
     * rewriters are added.
     *
     * <p>TODO: assess whether each of these rewriters genuinely needs to run at the Substrait
     * visitor layer, or whether the ones that only manipulate Calcite {@link RelNode}s (and don't
     * depend on DataFusion/Substrait-specific classes) can be lifted up into the analytics-engine
     * planner layer. Moving them up would let other backends reuse them and keep backend fragment
     * conversion mostly a shape-to-Substrait translation.
     */
    private static RelNode preprocessForSubstrait(RelNode rel) {
        RelNode preprocessed = CorrelateUncollectRewriter.rewrite(rel);
        preprocessed = UntypedNullPreprocessor.rewrite(preprocessed);
        preprocessed = PplAggregateCallRewriter.rewrite(preprocessed);
        preprocessed = PplWindowCallRewriter.rewrite(preprocessed);
        preprocessed = ItemTypeRebuilder.rewrite(preprocessed);
        preprocessed = CastToVarcharRewriter.rewrite(preprocessed);
        preprocessed = CastTemporalLiteralValidator.rewrite(preprocessed);
        return preprocessed;
    }

    private byte[] convertToSubstrait(RelNode fragment) {
        LOGGER.info("[NESTED-POC] convertToSubstrait: INPUT fragment:\n{}",
            org.apache.calcite.plan.RelOptUtil.toString(fragment));
        LOGGER.info("[NESTED-POC] convertToSubstrait: INPUT fragment row type: {}", fragment.getRowType());

        RelNode preprocessed = preprocessForSubstrait(fragment);
        LOGGER.info("[NESTED-POC] convertToSubstrait: AFTER preprocessForSubstrait:\n{}",
            org.apache.calcite.plan.RelOptUtil.toString(preprocessed));
        LOGGER.info("[NESTED-POC] convertToSubstrait: preprocessed row type: {}", preprocessed.getRowType());

        RelRoot root = RelRoot.of(preprocessed, SqlKind.SELECT);
        SubstraitRelVisitor visitor = createVisitor(preprocessed);
        Rel substraitRel;
        try {
            substraitRel = visitor.apply(root.rel);
        } catch (AssertionError e) {
            // Substrait validators throw AssertionError directly (not via `assert`), so -da
            // doesn't gate them; convert to a normal exception so we don't crash the cluster.
            throw new IllegalStateException("Substrait conversion rejected the plan: " + e.getMessage(), e);
        }

        // Substrait Root.names is a DEPTH-FIRST FLATTENED name list: DataFusion's consumer
        // (rename_data_type in datafusion-substrait) consumes one name per field INCLUDING
        // struct children and list-of-struct element fields. For scalar-only outputs this
        // equals the top-level list; for nested outputs it includes sub-field names.
        List<String> topLevelNames = root.fields.stream().map(field -> field.getValue()).toList();
        List<String> fieldNames = flattenOutputNames(topLevelNames, root.validatedRowType);

        Plan.Root substraitRoot = Plan.Root.builder().input(substraitRel).names(fieldNames).build();
        Plan plan = Plan.builder().addRoots(substraitRoot).build();

        plan = SubstraitPlanPojoRewriter.rewrite(plan);

        io.substrait.proto.Plan protoPlan = SubstraitPlanProtoRewriter.rewrite(new PlanProtoConverter().toProto(plan));

        // [NESTED-POC] If the CorrelateUncollectRewriter detected a Correlate+Uncollect pattern,
        // rebuild the entire plan: ReadRel → ExtensionSingleRel(unnest) → ProjectRel.
        // We bypass Isthmus' project emission because Isthmus can't serialize ITEM on LIST<STRUCT>.
        CorrelateUncollectRewriter.UnnestInfo unnestInfo = CorrelateUncollectRewriter.getUnnestInfo();
        if (unnestInfo != null) {
            CorrelateUncollectRewriter.clearUnnestInfo();
            protoPlan = buildUnnestPlan(protoPlan, unnestInfo);
            LOGGER.info("[NESTED-POC] Built complete unnest plan: ReadRel → ExtensionSingleRel(unnest:{}) → ProjectRel",
                unnestInfo.arrayColumnName());
        }

        byte[] bytes = protoPlan.toByteArray();
        LOGGER.info("[NESTED-POC] Substrait plan ({} bytes) [isthmus path]:\n{}", bytes.length, protoPlan);
        return bytes;
    }

    /**
     * [NESTED-POC] Builds the complete Substrait plan for an unnest query:
     * ReadRel → ExtensionSingleRel(unnest:column) → ProjectRel(select post-unnest fields).
     *
     * After double-unnest on the Rust side, the schema becomes:
     *   [struct_field_0, struct_field_1, ..., other_scan_cols...]
     * where the array column is replaced by its struct fields expanded in-place.
     *
     * The ProjectRel selects exactly the columns the original query requested.
     */
    private static io.substrait.proto.Plan buildUnnestPlan(io.substrait.proto.Plan isthmusPlan,
                                                            CorrelateUncollectRewriter.UnnestInfo info) {
        // Isthmus produced a plan with just the ReadRel (bare scan).
        // We need to: (1) wrap ReadRel with ExtensionSingleRel, (2) add ProjectRel on top.

        io.substrait.proto.Plan.Builder planBuilder = isthmusPlan.toBuilder();
        for (int i = 0; i < planBuilder.getRelationsCount(); i++) {
            io.substrait.proto.PlanRel planRel = planBuilder.getRelations(i);
            if (!planRel.hasRoot()) continue;

            io.substrait.proto.RelRoot root = planRel.getRoot();
            io.substrait.proto.Rel topRel = root.getInput();

            // Find the ReadRel (may be inside a Project that Isthmus added for column pruning)
            io.substrait.proto.Rel readRel = findReadRel(topRel);
            if (readRel == null) readRel = topRel;

            // Step 1: Wrap the ReadRel in ExtensionSingleRel
            io.substrait.proto.ExtensionSingleRel extensionRel = io.substrait.proto.ExtensionSingleRel.newBuilder()
                .setCommon(io.substrait.proto.RelCommon.newBuilder()
                    .setDirect(io.substrait.proto.RelCommon.Direct.getDefaultInstance())
                    .build())
                .setInput(readRel)
                .setDetail(com.google.protobuf.Any.newBuilder()
                    .setTypeUrl("unnest:" + info.arrayColumnName())
                    .build())
                .build();
            io.substrait.proto.Rel unnestRel = io.substrait.proto.Rel.newBuilder()
                .setExtensionSingle(extensionRel)
                .build();

            // Step 2: Build ProjectRel that selects the right post-unnest columns.
            // After double-unnest of "comments" (LIST<STRUCT<author,score>>), the schema is:
            //   [comments.author(0), comments.score(1), title(2), views(3)]
            // The array column at index K is replaced by structFieldCount fields starting at K.
            // Columns after K shift right by (structFieldCount - 1).
            int structFieldCount = info.structFieldNames().size();
            int arrayColIdx = info.arrayColumnIndex();

            io.substrait.proto.ProjectRel.Builder projectBuilder = io.substrait.proto.ProjectRel.newBuilder()
                .setCommon(io.substrait.proto.RelCommon.newBuilder()
                    .setDirect(io.substrait.proto.RelCommon.Direct.getDefaultInstance())
                    .build())
                .setInput(unnestRel);

            // Build field reference expressions for the output columns
            List<Integer> emitIndices = new ArrayList<>();
            int outputIdx = 0;
            // First, the input columns from unnest pass through (no extra expressions needed for emit-only)
            // We use emit mapping to select exactly the columns we want.
            // Post-unnest schema positions:
            //   - struct fields at positions: [arrayColIdx .. arrayColIdx + structFieldCount - 1]
            //   - original columns before array: [0 .. arrayColIdx - 1] → shifted to same positions? NO!
            //   After double unnest, position layout is:
            //     [struct_field_0, struct_field_1, ..., cols_before_array..., cols_after_array...]
            //   Wait — per Ansh's empirical finding:
            //     Base schema: [comments(0), title(1), views(2)]
            //     After unnest: [comments.author(0), comments.score(1), title(2), views(3)]
            //   So struct fields appear IN PLACE at the array's position.
            //   Columns after the array shift right by (structFieldCount - 1).

            // Build emit-only ProjectRel: just emit the indices we need
            io.substrait.proto.RelCommon.Builder emitCommon = io.substrait.proto.RelCommon.newBuilder()
                .setEmit(io.substrait.proto.RelCommon.Emit.newBuilder());

            io.substrait.proto.RelCommon.Emit.Builder emitBuilder =
                io.substrait.proto.RelCommon.Emit.newBuilder();

            for (int col = 0; col < info.unnestFieldIndices().length; col++) {
                int unnestFieldIdx = info.unnestFieldIndices()[col];
                int scanIdx = info.scanColIndices()[col];

                if (unnestFieldIdx >= 0) {
                    // This output is a struct field from unnest → position = arrayColIdx + unnestFieldIdx
                    emitBuilder.addOutputMapping(arrayColIdx + unnestFieldIdx);
                } else if (scanIdx >= 0) {
                    // This output is a pass-through scan column
                    int postUnnestPos;
                    if (scanIdx < arrayColIdx) {
                        // Before array: same position (unchanged by unnest? NO — struct fields expand in place)
                        // Actually per the empirical layout: struct fields take positions [arrayColIdx..],
                        // and the OTHER columns shift. Let me look at Ansh's comment:
                        //   Base: [comments(0), title(1), views(2)]
                        //   After: [comments.author(0), comments.score(1), title(2), views(3)]
                        // So columns AFTER the array shift by (structFieldCount - 1).
                        // Columns BEFORE the array... there are none in this example.
                        // In general: columns at index < arrayColIdx stay at their index? No — the
                        // struct fields expand AT the array's index, pushing everything after it.
                        // Actually the layout is simpler: the array col is REPLACED by struct fields.
                        // So: idx < arrayColIdx → same position (untouched).
                        postUnnestPos = scanIdx;
                    } else {
                        // After (or at) the array: shift right by (structFieldCount - 1)
                        postUnnestPos = scanIdx + structFieldCount - 1;
                    }
                    emitBuilder.addOutputMapping(postUnnestPos);
                }
            }

            // No emit/project — let the unnest return all columns.
            // Root.names describes the full post-unnest output; the Java coordinator
            // picks the columns it needs by matching against the plan's row type.
            io.substrait.proto.Rel projectRel = unnestRel;

            // Build output names for the Root — must match the FULL post-unnest schema.
            // After double-unnest of array column at index K with S struct fields:
            //   [struct_field_0, struct_field_1, ..., cols_after_array...]
            // The array column is replaced by its S struct fields in-place.
            List<String> outputNames = new ArrayList<>();
            io.substrait.proto.NamedStruct baseSchema = findReadRel(topRel) != null
                ? findReadRel(topRel).getRead().getBaseSchema() : null;
            if (baseSchema != null) {
                // Walk the original schema and expand the array column into struct field names
                int nameIdx = 0;
                for (int col2 = 0; col2 < baseSchema.getStruct().getTypesCount(); col2++) {
                    if (col2 == arrayColIdx) {
                        // This is the array column — replace with struct field names
                        for (String sf : info.structFieldNames()) {
                            outputNames.add(info.arrayColumnName() + "." + sf);
                        }
                        // Skip the nested names in the original schema (array + struct fields)
                        nameIdx++; // skip array name itself
                        nameIdx += info.structFieldNames().size(); // skip struct field names
                    } else {
                        outputNames.add(baseSchema.getNames(nameIdx));
                        nameIdx++;
                    }
                }
            } else {
                // Fallback: use original root names
                outputNames.addAll(root.getNamesList());
            }

            io.substrait.proto.RelRoot newRoot = io.substrait.proto.RelRoot.newBuilder()
                .setInput(projectRel)
                .addAllNames(outputNames)
                .build();
            planBuilder.setRelations(i, io.substrait.proto.PlanRel.newBuilder().setRoot(newRoot).build());
        }
        return planBuilder.build();
    }

    /** Finds the ReadRel anywhere in a single-input chain. */
    private static io.substrait.proto.Rel findReadRel(io.substrait.proto.Rel rel) {
        if (rel.hasRead()) return rel;
        if (rel.hasProject()) return findReadRel(rel.getProject().getInput());
        if (rel.hasFilter()) return findReadRel(rel.getFilter().getInput());
        if (rel.hasFetch()) return findReadRel(rel.getFetch().getInput());
        if (rel.hasSort()) return findReadRel(rel.getSort().getInput());
        return null;
    }

    /**
     * Flattens output field names depth-first: for each top-level field, if it's a struct
     * or ARRAY(struct), appends the struct's child field names recursively. This matches
     * what DataFusion's Substrait consumer expects in the Root.names list.
     *
     * <p>Example: output type (title:VARCHAR, comments:ARRAY(ROW(author:VARCHAR, score:INTEGER)))
     * → flattened names: ["title", "comments", "author", "score"]
     */
    private static List<String> flattenOutputNames(List<String> topLevelNames, RelDataType rowType) {
        List<String> result = new java.util.ArrayList<>();
        List<RelDataTypeField> fields = rowType.getFieldList();
        for (int i = 0; i < topLevelNames.size(); i++) {
            result.add(topLevelNames.get(i));
            if (i < fields.size()) {
                RelDataType fieldType = fields.get(i).getType();
                flattenStructNames(fieldType, result);
            }
        }
        return result;
    }

    /** Recursively appends struct child names for nested types. */
    private static void flattenStructNames(RelDataType type, List<String> out) {
        // ARRAY(ROW(...)) — descend into the element type
        if (type.getSqlTypeName() == SqlTypeName.ARRAY) {
            RelDataType componentType = type.getComponentType();
            if (componentType != null) {
                flattenStructNames(componentType, out);
            }
            return;
        }
        // ROW/STRUCT — append each child name, then recurse into children
        if (type.getSqlTypeName() == SqlTypeName.ROW) {
            for (RelDataTypeField child : type.getFieldList()) {
                out.add(child.getName());
                flattenStructNames(child.getType(), out);
            }
        }
    }

    /** Converts a single operator into a Substrait {@link Rel}; children are discarded and rewired by {@link #rewire}. */
    private Rel convertStandalone(RelNode operator) {
        RelNode preprocessed = preprocessForSubstrait(operator);
        SubstraitRelVisitor visitor = createVisitor(preprocessed);
        return visitor.apply(preprocessed);
    }

    /** Rewires {@code wrapper} above {@code inner}'s root; {@code wrapperNames} must match the wrapper's output schema. */
    static Plan rewire(Plan inner, Rel wrapper, List<String> wrapperNames) {
        if (inner.getRoots().isEmpty()) {
            throw new IllegalArgumentException("Inner Substrait plan has no root relation to rewire under wrapper");
        }
        Plan.Root innerRoot = inner.getRoots().get(0);
        Rel innerRel = innerRoot.getInput();
        Rel rewired = replaceInput(wrapper, innerRel);
        return Plan.builder().addRoots(Plan.Root.builder().input(rewired).names(wrapperNames).build()).build();
    }

    /** Wrapper's output column names from its Calcite row type. */
    private static List<String> fieldNames(RelNode fragment) {
        return fragment.getRowType().getFieldList().stream().map(RelDataTypeField::getName).toList();
    }

    private static Rel replaceInput(Rel wrapper, Rel newInput) {
        if (wrapper instanceof Aggregate agg) {
            return Aggregate.builder().from(agg).input(newInput).build();
        }
        if (wrapper instanceof Sort sort) {
            return Sort.builder().from(sort).input(newInput).build();
        }
        if (wrapper instanceof Filter filter) {
            return Filter.builder().from(filter).input(newInput).build();
        }
        if (wrapper instanceof Project project) {
            // Lifted-window shape: outer Project references a window column from the lower Project.
            if (project.getInput() instanceof Project lower && containsWindowFunction(lower)) {
                Rel rewiredLower = replaceInput(lower, newInput);
                return Project.builder().from(project).input(rewiredLower).build();
            }
            return Project.builder().from(project).input(newInput).build();
        }
        if (wrapper instanceof Fetch fetch) {
            // A single Calcite LogicalSort carrying both a collation AND a fetch/offset lowers to
            // Fetch(Sort(input)) — two Substrait rels from one node. Rewiring the Fetch's input
            // directly would drop the Sort and lose global order before the limit. Descend into
            // the Sort so the shape becomes Fetch(Sort(newInput)): gather, sort globally, then limit.
            Rel rewiredInput = fetch.getInput() instanceof Sort ? replaceInput(fetch.getInput(), newInput) : newInput;
            return Fetch.builder().from(fetch).input(rewiredInput).build();
        }
        throw new UnsupportedOperationException(
            "Cannot attach-on-top a Substrait Rel of type " + wrapper.getClass().getSimpleName() + " — no single-input rewire defined"
        );
    }

    private static boolean containsWindowFunction(Project project) {
        for (Expression expr : project.getExpressions()) {
            if (expr instanceof Expression.WindowFunctionInvocation) {
                return true;
            }
        }
        return false;
    }

    /** Forces {@code phase} on every measure of an Aggregate wrapper (isthmus hardcodes INITIAL_TO_RESULT). */
    private static Rel withAggregationPhase(Rel rel, Expression.AggregationPhase phase) {
        if (!(rel instanceof Aggregate agg)) {
            return rel;
        }
        List<Aggregate.Measure> newMeasures = new ArrayList<>(agg.getMeasures().size());
        for (Aggregate.Measure m : agg.getMeasures()) {
            AggregateFunctionInvocation fn = m.getFunction();
            AggregateFunctionInvocation rephased = AggregateFunctionInvocation.builder().from(fn).aggregationPhase(phase).build();
            newMeasures.add(Aggregate.Measure.builder().from(m).function(rephased).build());
        }
        return Aggregate.builder().from(agg).measures(newMeasures).build();
    }

    /** Rewrites {@link OpenSearchStageInputScan} leaves to TableScan with {@code "input-<childStageId>"} names. */
    private static RelNode rewriteStageInputScans(RelNode node) {
        if (node instanceof OpenSearchStageInputScan scan) {
            return new StageInputTableScan(scan.getCluster(), scan.getTraitSet(), "input-" + scan.getChildStageId(), scan.getRowType());
        }
        List<RelNode> newInputs = new ArrayList<>(node.getInputs().size());
        boolean changed = false;
        for (RelNode input : node.getInputs()) {
            RelNode rewritten = rewriteStageInputScans(input);
            newInputs.add(rewritten);
            if (rewritten != input) {
                changed = true;
            }
        }
        if (changed) {
            return node.copy(node.getTraitSet(), newInputs);
        }
        return node;
    }

    // ── Visitor wiring ──────────────────────────────────────────────────────────

    private SubstraitRelVisitor createVisitor(RelNode relNode) {
        RelDataTypeFactory typeFactory = relNode.getCluster().getTypeFactory();
        TypeConverter typeConverter = TypeConverter.DEFAULT;
        ScalarFunctionConverter scalarConverter = new ScalarFunctionConverter(
            extensions.scalarFunctions(),
            ADDITIONAL_SCALAR_SIGS,
            typeFactory,
            typeConverter
        ) {
            @Override
            public Optional<io.substrait.expression.Expression> convert(
                org.apache.calcite.rex.RexCall call,
                java.util.function.Function<org.apache.calcite.rex.RexNode, io.substrait.expression.Expression> topLevelConverter
            ) {
                // POC nested (N1): handle unnest_field(col_index, field_name) directly
                // Build a Substrait ScalarFunction without YAML signature lookup
                if ("unnest_field".equals(call.getOperator().getName())) {
                    SimpleExtension.ScalarFunctionVariant variant = extensions.scalarFunctions()
                        .stream()
                        .filter(f -> "unnest_field".equals(f.name()))
                        .findFirst()
                        .orElse(null);
                    if (variant == null) {
                        // Function not in YAML — build with any variant we have and use name only
                        // Emit as generic function with literal args; Rust resolves by name
                        List<io.substrait.expression.Expression> args = new java.util.ArrayList<>();
                        for (org.apache.calcite.rex.RexNode operand : call.getOperands()) {
                            args.add(topLevelConverter.apply(operand));
                        }
                        // Use a dummy variant from the first scalar function as template
                        SimpleExtension.ScalarFunctionVariant dummyVariant = extensions.scalarFunctions().get(0);
                        return Optional.of(
                            io.substrait.expression.ImmutableExpression.ScalarFunctionInvocation.builder()
                                .declaration(dummyVariant)
                                .addAllArguments(args)
                                .outputType(io.substrait.type.TypeCreator.NULLABLE.STRING)
                                .build()
                        );
                    }
                    List<io.substrait.expression.Expression> args = new java.util.ArrayList<>();
                    for (org.apache.calcite.rex.RexNode operand : call.getOperands()) {
                        args.add(topLevelConverter.apply(operand));
                    }
                    return Optional.of(
                        io.substrait.expression.ImmutableExpression.ScalarFunctionInvocation.builder()
                            .declaration(variant)
                            .addAllArguments(args)
                            .outputType(io.substrait.type.TypeCreator.NULLABLE.STRING)
                            .build()
                    );
                }
                return super.convert(call, topLevelConverter);
            }
        };
        // Filter isthmus's default APPROX_COUNT_DISTINCT binding so our `approx_distinct` entry wins.
        // The convert() override inlines literal-Project columns into the AggregateFunctionInvocation
        // as Substrait literals so two-stage UDAFs (e.g. TAKE's N) see the constant on the Final side.
        AggregateFunctionConverter aggConverter = new AggregateFunctionConverter(
            extensions.aggregateFunctions(),
            ADDITIONAL_AGGREGATE_SIGS,
            typeFactory,
            typeConverter
        ) {
            @Override
            protected ImmutableList<FunctionMappings.Sig> getSigs() {
                return super.getSigs().stream()
                    .filter(sig -> sig.operator != SqlStdOperatorTable.APPROX_COUNT_DISTINCT)
                    .collect(ImmutableList.toImmutableList());
            }

            @Override
            public Optional<AggregateFunctionInvocation> convert(
                RelNode input,
                Type.Struct inputType,
                AggregateCall call,
                Function<RexNode, Expression> rexConverter
            ) {
                Optional<AggregateFunctionInvocation> bound = super.convert(input, inputType, call, rexConverter);
                if (bound.isEmpty()) {
                    return bound;
                }
                // Let the op rewrite its data args (e.g. a type-coercing CAST) on the bound Substrait
                // argument — generic dispatch; the cast semantics live on the LocalAggOp, not here.
                Optional<AggregateFunctionInvocation> rewrittenArgs = rewriteLocalAggDataArgs(input, call, bound.get(), rexConverter);
                if (rewrittenArgs.isPresent()) {
                    return rewrittenArgs;
                }
                if (!(input instanceof org.apache.calcite.rel.core.Project project)) {
                    return bound;
                }
                AggregateFunctionInvocation fn = bound.get();
                List<RexNode> projects = project.getProjects();
                List<FunctionArg> args = fn.arguments();
                List<FunctionArg> rewritten = null;
                RexBuilder rexBuilder = project.getCluster().getRexBuilder();
                for (int i = 0; i < args.size(); i++) {
                    FunctionArg arg = args.get(i);
                    if (!(arg instanceof io.substrait.expression.FieldReference fr)) continue;
                    Integer offset = simpleStructOffset(fr);
                    if (offset == null || offset < 0 || offset >= projects.size()) continue;
                    if (!(projects.get(offset) instanceof RexLiteral rexLit)) continue;
                    if (rewritten == null) rewritten = new ArrayList<>(args);
                    RexNode toConvert = call.getAggregation() instanceof LocalAggOp localOp
                        ? localOp.normaliseLiteralArg(i, rexLit, rexBuilder, typeFactory)
                        : rexLit;
                    rewritten.set(i, rexConverter.apply(toConvert));
                }
                if (rewritten == null) return bound;
                return Optional.of(ImmutableAggregateFunctionInvocation.builder().from(fn).arguments(rewritten).build());
            }
        };
        // Same APPROX_COUNT_DISTINCT filter as aggConverter — let our `approx_distinct` entry win.
        WindowFunctionConverter windowConverter = new WindowFunctionConverter(
            extensions.windowFunctions(),
            ADDITIONAL_WINDOW_SIGS,
            typeFactory,
            typeConverter
        ) {
            @Override
            protected ImmutableList<FunctionMappings.Sig> getSigs() {
                return super.getSigs().stream()
                    .filter(sig -> sig.operator != SqlStdOperatorTable.APPROX_COUNT_DISTINCT)
                    .collect(ImmutableList.toImmutableList());
            }
        };
        ConverterProvider converterProvider = new ConverterProvider(
            typeFactory,
            extensions,
            scalarConverter,
            aggConverter,
            windowConverter,
            typeConverter
        );
        return new SubstraitRelVisitor(converterProvider) {
            @Override
            public Rel visit(org.apache.calcite.rel.core.Aggregate aggregate) {
                Rel rel = super.visit(aggregate);
                return rel instanceof Aggregate agg ? addNullArgFilters(aggregate, agg) : rel;
            }
        };
    }

    /**
     * Adds an {@code is_not_null} {@code preMeasureFilter} to each measure whose {@link LocalAggOp}
     * declares {@link LocalAggOp#filtersNullArgs} — so the converter stays generic and only the op
     * opts in (DataFusion's substrait consumer can't take the function's own {@code ignore_nulls}).
     * Measures line up with the Calcite agg calls minus any {@code GROUP_ID()} (which isthmus drops).
     */
    private Aggregate addNullArgFilters(org.apache.calcite.rel.core.Aggregate calcite, Aggregate agg) {
        List<AggregateCall> calls = calcite.getAggCallList()
            .stream()
            .filter(c -> c.getAggregation() != SqlStdOperatorTable.GROUP_ID)
            .toList();
        List<Aggregate.Measure> measures = agg.getMeasures();
        if (calls.size() != measures.size()) {
            return agg; // shape we don't recognise — leave untouched
        }
        List<Aggregate.Measure> rewritten = null;
        for (int i = 0; i < measures.size(); i++) {
            Aggregate.Measure m = measures.get(i);
            if (!(calls.get(i).getAggregation() instanceof LocalAggOp op) || !op.filtersNullArgs(calls.get(i))) {
                continue;
            }
            if (m.getPreMeasureFilter().isPresent()
                || m.getFunction().arguments().isEmpty()
                || !(m.getFunction().arguments().get(0) instanceof Expression argExpr)) {
                continue;
            }
            Expression filter = isNotNull(argExpr);
            if (filter == null) {
                continue;
            }
            if (rewritten == null) {
                rewritten = new ArrayList<>(measures);
            }
            rewritten.set(i, Aggregate.Measure.builder().from(m).preMeasureFilter(filter).build());
        }
        return rewritten == null ? agg : Aggregate.builder().from(agg).measures(rewritten).build();
    }

    /** Builds {@code is_not_null(arg)} from the merged extension catalog, or null if the variant is absent. */
    private Expression isNotNull(Expression arg) {
        SimpleExtension.ScalarFunctionVariant variant = extensions.scalarFunctions()
            .stream()
            .filter(f -> "is_not_null".equals(f.name()))
            .findFirst()
            .orElse(null);
        if (variant == null) {
            return null;
        }
        return io.substrait.expression.ImmutableExpression.ScalarFunctionInvocation.builder()
            .declaration(variant)
            .addArguments(arg)
            .outputType(io.substrait.type.TypeCreator.REQUIRED.BOOLEAN)
            .build();
    }

    /** Column offset for a simple input-rooted single-segment {@code StructField}, else null. */
    private static Integer simpleStructOffset(io.substrait.expression.FieldReference fr) {
        if (fr.isOuterReference() || fr.isLambdaParameterReference()) return null;
        if (!fr.inputExpression().isEmpty()) return null;
        if (fr.segments().size() != 1) return null;
        io.substrait.expression.FieldReference.ReferenceSegment seg = fr.segments().get(0);
        if (!(seg instanceof io.substrait.expression.FieldReference.StructField sf)) return null;
        return sf.offset();
    }

    /**
     * Lets a {@link LocalAggOp} rewrite its data args on the bound Substrait invocation (e.g. a
     * type-coercing CAST), keyed only on the generic hook — no per-function logic here. Returns
     * empty when the op is not a {@code LocalAggOp} or leaves every arg unchanged.
     */
    private Optional<AggregateFunctionInvocation> rewriteLocalAggDataArgs(
        RelNode input,
        AggregateCall call,
        AggregateFunctionInvocation fn,
        Function<RexNode, Expression> rexConverter
    ) {
        if (!(call.getAggregation() instanceof LocalAggOp op)) {
            return Optional.empty();
        }
        RexBuilder rexBuilder = input.getCluster().getRexBuilder();
        RelDataTypeFactory typeFactory = input.getCluster().getTypeFactory();
        List<FunctionArg> rewritten = null;
        for (int i = 0; i < call.getArgList().size(); i++) {
            RelDataType srcType = input.getRowType().getFieldList().get(call.getArgList().get(i)).getType();
            RexNode argRef = rexBuilder.makeInputRef(srcType, call.getArgList().get(i));
            Optional<RexNode> replacement = op.rewriteDataArg(i, argRef, rexBuilder, typeFactory);
            if (replacement.isEmpty()) {
                continue;
            }
            if (rewritten == null) {
                rewritten = new ArrayList<>(fn.arguments());
            }
            rewritten.set(i, rexConverter.apply(replacement.get()));
        }
        List<FunctionArg> args = rewritten != null ? rewritten : fn.arguments();
        // Sort the elements ascending by the (rewritten) first arg when the op asks for it — emitted
        // as the invocation's sort, which DataFusion's array_agg honours (its DISTINCT+ORDER BY rule
        // is satisfied because the sort key IS the argument expression).
        List<Expression.SortField> sorts = fn.sort();
        boolean addedSort = false;
        if (op.sortsArgAscending(call) && sorts.isEmpty() && !args.isEmpty() && args.get(0) instanceof Expression sortKey) {
            sorts = List.of(
                io.substrait.expression.ImmutableExpression.SortField.builder()
                    .expr(sortKey)
                    .direction(Expression.SortDirection.ASC_NULLS_LAST)
                    .build()
            );
            addedSort = true;
        }
        if (rewritten == null && !addedSort) {
            return Optional.empty();
        }
        return Optional.of(ImmutableAggregateFunctionInvocation.builder().from(fn).arguments(args).sort(sorts).build());
    }

    /**
     * Local aggregate stub that may transform inlined literal args before substrait emission.
     * Other local stubs without transformations stay as plain {@link SqlAggFunction}; the
     * {@code convert()} override only invokes {@link #normaliseLiteralArg} when the call's
     * operator is a {@code LocalAggOp}, so adding a new normalisation is purely a matter of
     * subclassing here next to the op's declaration.
     */
    abstract static class LocalAggOp extends SqlAggFunction {
        LocalAggOp(
            String name,
            SqlKind kind,
            org.apache.calcite.sql.type.SqlReturnTypeInference returnTypeInference,
            org.apache.calcite.sql.type.SqlOperandTypeChecker operandTypeChecker
        ) {
            super(
                name,
                null,
                kind,
                returnTypeInference,
                null,
                operandTypeChecker,
                SqlFunctionCategory.USER_DEFINED_FUNCTION,
                false,
                false,
                Optionality.FORBIDDEN
            );
        }

        /** Identity by default; override to transform the {@code argIndex}-th inlined literal arg. */
        public RexNode normaliseLiteralArg(int argIndex, RexLiteral lit, RexBuilder rexBuilder, RelDataTypeFactory typeFactory) {
            return lit;
        }

        /**
         * Returns the expression to emit for the {@code argIndex}-th data arg in place of a bare
         * field reference (e.g. a type-coercing CAST), or empty to keep the reference. Applied on
         * the bound Substrait argument, so it rides the measure without a child Project that the
         * reduce-stage stitch ({@link #replaceInput}) would drop. Identity by default.
         */
        public Optional<RexNode> rewriteDataArg(int argIndex, RexNode argRef, RexBuilder rexBuilder, RelDataTypeFactory typeFactory) {
            return Optional.empty();
        }

        /**
         * Whether the aggregate's elements are emitted ascending-sorted by the (rewritten) data arg.
         * Carried as the invocation's sort, which DataFusion's {@code array_agg} honours. False by default.
         */
        public boolean sortsArgAscending(AggregateCall call) {
            return false;
        }

        /**
         * Whether null arguments are dropped before aggregating. Carried as the measure's
         * {@code is_not_null} preMeasureFilter (DataFusion's substrait consumer can't take the
         * function's own {@code ignore_nulls}). False by default.
         */
        public boolean filtersNullArgs(AggregateCall call) {
            return false;
        }
    }

    // ── Plan serde helpers ──────────────────────────────────────────────────────

    /** Decodes serialized Substrait bytes into a model-level {@link Plan}. */
    private Plan decodePlan(byte[] bytes) {
        try {
            io.substrait.proto.Plan proto = io.substrait.proto.Plan.parseFrom(bytes);
            return new ProtoPlanConverter(extensions).from(proto);
        } catch (InvalidProtocolBufferException e) {
            throw new IllegalArgumentException("Failed to decode Substrait plan bytes", e);
        }
    }

    /** Serializes a model-level {@link Plan} to proto bytes. */
    private static byte[] serializePlan(Plan plan) {
        return SubstraitPlanProtoRewriter.rewrite(new PlanProtoConverter().toProto(plan)).toByteArray();
    }

    // ── Calcite TableScan wrappers for OpenSearchStageInputScan rewrite ─────────

    static final class StageInputTableScan extends TableScan {
        StageInputTableScan(RelOptCluster cluster, RelTraitSet traitSet, String stageInputId, RelDataType rowType) {
            super(cluster, traitSet, List.of(), new StageInputRelOptTable(stageInputId, rowType));
        }
    }

    static final class StageInputRelOptTable implements RelOptTable {
        private final List<String> qualifiedName;
        private final RelDataType rowType;

        StageInputRelOptTable(String stageInputId, RelDataType rowType) {
            this.qualifiedName = List.of(stageInputId);
            this.rowType = rowType;
        }

        @Override
        public List<String> getQualifiedName() {
            return qualifiedName;
        }

        @Override
        public RelDataType getRowType() {
            return rowType;
        }

        @Override
        public double getRowCount() {
            return 100;
        }

        @Override
        public RelOptSchema getRelOptSchema() {
            return null;
        }

        @Override
        public RelNode toRel(ToRelContext context) {
            throw new UnsupportedOperationException("StageInputRelOptTable.toRel not supported");
        }

        @Override
        public List<ColumnStrategy> getColumnStrategies() {
            return List.of();
        }

        @Override
        public <C> C unwrap(Class<C> aClass) {
            return null;
        }

        @Override
        public boolean isKey(ImmutableBitSet columns) {
            return false;
        }

        @Override
        public List<ImmutableBitSet> getKeys() {
            return List.of();
        }

        @Override
        public List<RelReferentialConstraint> getReferentialConstraints() {
            return List.of();
        }

        @Override
        public List<RelCollation> getCollationList() {
            return List.of();
        }

        @Override
        public RelDistribution getDistribution() {
            return RelDistributions.ANY;
        }

        @Override
        @SuppressWarnings("rawtypes")
        public org.apache.calcite.linq4j.tree.Expression getExpression(Class clazz) {
            return null;
        }

        @Override
        public RelOptTable extend(List<RelDataTypeField> extendedFields) {
            return this;
        }
    }
}
