package com.datasqrl.calcite;

import static com.datasqrl.plan.validate.ScriptValidator.isSelfField;
import static com.datasqrl.plan.validate.ScriptValidator.isSelfTable;
import static com.datasqrl.plan.validate.ScriptValidator.isVariable;

import com.datasqrl.calcite.SqrlToSql.Context;
import com.datasqrl.calcite.SqrlToSql.Result;
import com.datasqrl.calcite.schema.PathWalker;
import com.datasqrl.calcite.schema.sql.SqlBuilders.SqlAliasCallBuilder;
import com.datasqrl.calcite.schema.sql.SqlBuilders.SqlCallBuilder;
import com.datasqrl.calcite.schema.sql.SqlBuilders.SqlJoinBuilder;
import com.datasqrl.calcite.schema.sql.SqlBuilders.SqlSelectBuilder;
import com.datasqrl.calcite.schema.sql.SqlDataTypeSpecBuilder;
import com.datasqrl.calcite.schema.sql.SqlJoinPathBuilder;
import com.datasqrl.calcite.type.TypeFactory;
import com.datasqrl.calcite.visitor.SqlNodeVisitor;
import com.datasqrl.calcite.visitor.SqlRelationVisitor;
import com.datasqrl.canonicalizer.Name;
import com.datasqrl.canonicalizer.ReservedName;
import com.datasqrl.function.SqrlFunctionParameter;
import com.datasqrl.plan.hints.TopNHint.Type;
import com.datasqrl.util.SqlNameUtil;
import com.google.common.base.Preconditions;
import com.google.common.collect.ArrayListMultimap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Value;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelRecordType;
import org.apache.calcite.schema.FunctionParameter;
import org.apache.calcite.schema.TableFunction;
import org.apache.calcite.sql.CalciteFixes;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDynamicParam;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlHint;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlSelectKeyword;
import org.apache.calcite.sql.SqlSyntax;
import org.apache.calcite.sql.SqlUnresolvedFunction;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.util.SqlShuttle;
import org.apache.calcite.sql.validate.SqlUserDefinedTableFunction;

public class SqrlToSql implements SqlRelationVisitor<Result, Context> {
  final TypeFactory typeFactory;
  final CatalogReader catalogReader;
  final SqlNameUtil nameUtil;
  final SqlOperatorTable operatorTable;
  final Map<SqlNode, SqlDynamicParam> dynamicParam;
  final AtomicInteger uniquePkId;
  final ArrayListMultimap<SqlNode, FunctionParameter> parameters;
  @Getter
  final List<FunctionParameter> params;
  final Map<FunctionParameter, SqlDynamicParam> paramMapping;

  public SqrlToSql(TypeFactory typeFactory, CatalogReader catalogReader, SqlNameUtil nameUtil,
      SqlOperatorTable operatorTable, Map<SqlNode, SqlDynamicParam> dynamicParam,
      AtomicInteger uniquePkId, ArrayListMultimap<SqlNode, FunctionParameter> parameters,
      List<FunctionParameter> mutableParams, Map<FunctionParameter, SqlDynamicParam> paramMapping) {
    this.typeFactory = typeFactory;
    this.catalogReader = catalogReader;
    this.nameUtil = nameUtil;
    this.operatorTable = operatorTable;
    this.dynamicParam = dynamicParam;
    this.uniquePkId = uniquePkId;
    this.parameters = parameters;
    this.params = new ArrayList<>(mutableParams);
    this.paramMapping = paramMapping;
  }

  public Result rewrite(SqlNode query, boolean materializeSelf, List<String> currentPath) {
    Context context = new Context(materializeSelf, currentPath, new HashMap<>(), false,
        currentPath.size() > 0, false);

    Result result = SqlNodeVisitor.accept(this, query, context);
    CalciteFixes.appendSelectLists(result.getSqlNode());
    return result;
  }

  @Override
  public Result visitQuerySpecification(SqlSelect call, Context context) {
    boolean isAggregating = hasAggs(call.getSelectList().getList());
    // Copy query specification with new RelNode.
    Context newContext = new Context(context.materializeSelf, context.currentPath, new HashMap<>(),
        isAggregating,
        context.isNested, call.getFetch() != null);
    Result result = SqlNodeVisitor.accept(this, appendAliasIfRequired(call.getFrom()), newContext);

    //todo get select list from validator

    //retain distinct hint too
    if (isDistinctOnHintPresent(call)) {
      List<Integer> hintOps = IntStream.range(0, call.getSelectList().size())
          .boxed()
          .collect(Collectors.toList());

      //create new sql node list
      SqlSelectBuilder sqlSelectBuilder = new SqlSelectBuilder(call)
          .setLimit(1)
          .clearKeywords()
          .setFrom(result.sqlNode);

      Set<String> fieldNames = new HashSet<>(getFieldNames(call.getSelectList().getList()));
      List<SqlNode> selectList = new ArrayList<>(call.getSelectList().getList());

      List<String> columns = catalogReader.getTableFromPath(result.getCurrentPath())
          .unwrap(ModifiableTable.class)
          .getRowType().getFieldNames();

      //Exclude columns
      Set<String> seenNames = new HashSet<>();
      seenNames.addAll(fieldNames.stream()
          .map(n -> nameUtil.toName(n).getDisplay())
          .collect(Collectors.toList()));

      List<SqlNode> newNodes = new ArrayList<>();
      //Walk backwards to get the latest nodes
      for (int i = columns.size() - 1; i >= 0; i--) {
        String column = columns.get(i);
        int idx = catalogReader.nameMatcher()
            .indexOf(seenNames, column);
        if (idx == -1) {
          seenNames.add(column);
          newNodes.add(new SqlIdentifier(column, SqlParserPos.ZERO));
        }
      }
      Collections.reverse(newNodes);
      selectList.addAll(newNodes);

      sqlSelectBuilder.setSelectList(selectList)
          .clearHints();
      result.condition
          .ifPresent(sqlSelectBuilder::appendWhere);
      SqlSelect top = new SqlSelectBuilder()
          .setFrom(sqlSelectBuilder.build())
          .setDistinctOnHint(hintOps)
          .build();

      return new Result(top, result.getCurrentPath(), List.of(), List.of(), Optional.empty());
    } else if (call.isKeywordPresent(SqlSelectKeyword.DISTINCT) ||
        (context.isNested() && call.getFetch() != null)) {
      //if is nested, get primary key nodes
      int keySize = context.isNested()
          ? catalogReader.getTableFromPath(context.currentPath).getKeys().get(0)
          .asSet().size()
          : 0;

      SqlSelectBuilder inner = new SqlSelectBuilder(call)
          .clearKeywords()
          .setFrom(result.getSqlNode())
          .rewriteExpressions(new WalkExpressions(newContext));
      pullUpKeys(inner, result.pullupColumns, isAggregating);
      result.condition
          .ifPresent(inner::appendWhere);
      SqlSelectBuilder topSelect = new SqlSelectBuilder()
          .setFrom(inner.build())
          .setTopNHint(call.isKeywordPresent(SqlSelectKeyword.DISTINCT)
              ? Type.SELECT_DISTINCT : Type.TOP_N, SqlSelectBuilder.sqlIntRange(keySize));

      return new Result(topSelect.build(),
          result.getCurrentPath(), List.of(), List.of(),
          Optional.empty());
    }

    SqlSelectBuilder select = new SqlSelectBuilder(call)
        .setFrom(result.getSqlNode())
        .rewriteExpressions(new WalkExpressions(newContext));

    result.condition
        .ifPresent(select::appendWhere);

    pullUpKeys(select, result.pullupColumns, isAggregating);

    return new Result(select.build(), result.getCurrentPath(), List.of(), List.of(), Optional.empty());
  }


  private SqlNode appendAliasIfRequired(SqlNode sqlNode) {
    if (sqlNode instanceof SqlIdentifier) {
      if (((SqlIdentifier) sqlNode).names.size() == 1) {
        return SqlStdOperatorTable.AS.createCall(sqlNode.getParserPosition(),
            sqlNode,
            sqlNode);
      }
    }

    if (sqlNode instanceof SqlIdentifier && ((SqlIdentifier) sqlNode).names.size() == 1) {
      return SqlStdOperatorTable.AS.createCall(sqlNode.getParserPosition(),
          sqlNode, sqlNode);
    }

    return sqlNode;
  }

  private List<String> getFieldNames(List<SqlNode> list) {
    List<String> nodes = new ArrayList<>();
    for (SqlNode node : list) {
      if (node instanceof SqlIdentifier) {
        String name = ((SqlIdentifier) node).names.get(((SqlIdentifier) node).names.size() - 1);
        nodes.add(nameUtil.toName(name).getCanonical());
      } else if (node instanceof SqlCall && node.getKind() == SqlKind.AS) {
        String name = ((SqlIdentifier) ((SqlCall) node).getOperandList().get(1)).names.get(0);
        nodes.add(nameUtil.toName(name).getCanonical());
      } else {
        throw new RuntimeException("Could not derive name: " + node);
      }
    }

    return nodes;
  }

  private boolean isDistinctOnHintPresent(SqlSelect call) {
    return call.getHints().getList().stream()
        .anyMatch(f -> ((SqlHint) f).getName().equalsIgnoreCase("DISTINCT_ON"));
  }

  private void pullUpKeys(SqlSelectBuilder inner, List<PullupColumn> keysToPullUp,
      boolean isAggregating) {
    if (!keysToPullUp.isEmpty()) {
      inner.prependSelect(keysToPullUp);
      if (isAggregating) {
        if (inner.hasOrder()) {
          inner.prependOrder(keysToPullUp);
        }
        inner.prependGroup(keysToPullUp);
      }
    }
  }

  private boolean hasAggs(List<SqlNode> list) {
    AtomicBoolean b = new AtomicBoolean(false);
    for (SqlNode node : list) {
      node.accept(new SqlShuttle() {
        @Override
        public SqlNode visit(SqlCall call) {
          if (call.getOperator() instanceof SqlUnresolvedFunction) {
            List<SqlOperator> matches = new ArrayList<>();
            operatorTable.lookupOperatorOverloads(call.getOperator().getNameAsId(),
                SqlFunctionCategory.USER_DEFINED_FUNCTION, SqlSyntax.FUNCTION, matches,
                catalogReader
                    .nameMatcher());

            for (SqlOperator op : matches) {
              if (op.isAggregator()) {
                b.set(true);
              }
            }
          } else {
            if (call.getOperator().isAggregator()) {
              b.set(true);
            }
          }
          return super.visit(call);
        }
      });
    }

    return b.get();
  }

  @Override
  public Result visitAliasedRelation(SqlCall node, Context context) {
    Result result = SqlNodeVisitor.accept(this, node.getOperandList().get(0), context);

    SqlAliasCallBuilder aliasBuilder = new SqlAliasCallBuilder(node);
    context.addAlias(aliasBuilder.getAlias(), result.getCurrentPath());

    SqlNode newNode = aliasBuilder.setTable(result.getSqlNode())
        .build();

    return new Result(newNode, result.getCurrentPath(), result.pullupColumns, List.of(), result.getCondition());
  }

  @Override
  public Result visitTable(SqlIdentifier node, Context context) {
    List<SqlNode> items = new ArrayList<>();
    for (int i = 0; i < node.names.size(); i++) {
      items.add(new SqlIdentifier(node.names.get(i), node.getComponentParserPosition(i)));
    }

    Iterator<SqlNode> input = items.iterator();
    PathWalker pathWalker = new PathWalker(catalogReader);

    SqlNode item = input.next();

    String identifier = getIdentifier(item)
        .orElseThrow(() -> new RuntimeException("Subqueries are not yet implemented"));

    SqlJoinPathBuilder builder = new SqlJoinPathBuilder(catalogReader);
    boolean isAlias = context.hasAlias(identifier);
    boolean isNested = context.isNested();
    boolean isSelf = identifier.equals(ReservedName.SELF_IDENTIFIER.getCanonical());
    boolean materializeSelf = context.isMaterializeSelf();
    SqlUserDefinedTableFunction tableFunction = catalogReader.getTableFunction(List.of(identifier));

    List<PullupColumn> pullupColumns = List.of();
    if (item.getKind() == SqlKind.SELECT) {
      SqrlToSql sqrlToSql = new SqrlToSql(typeFactory, catalogReader, nameUtil, operatorTable, dynamicParam,
          uniquePkId, parameters, List.of(), Map.of());
      Result rewrite = sqrlToSql.rewrite(item, false, context.currentPath);

      builder.pushSubquery(rewrite.getSqlNode(), new RelRecordType(List.of())/*can be empty*/);
    } else if (tableFunction != null) { //may be schema table or function
      pathWalker.walk(identifier);
      builder.scanFunction(pathWalker.getPath(), List.of());
    } else if (isAlias) {
      if (!input.hasNext()) {
        throw new RuntimeException("Alias by itself.");
      }

      pathWalker.setPath(context.getAliasPath(identifier));
      //Walk the next one and push in table function
      item = input.next();
      String nextIdentifier = getIdentifier(item)
          .orElseThrow(() -> new RuntimeException("Subqueries are not yet implemented"));

      pathWalker.walk(nextIdentifier);

      SqlUserDefinedTableFunction fnc = catalogReader.getTableFunction(pathWalker.getPath());
      List<SqlNode> args = rewriteArgs(identifier, fnc.getFunction(), context.materializeSelf);
      builder.scanFunction(fnc, args);
    } else if (isSelf) {
      pathWalker.setPath(context.getCurrentPath());
      if (materializeSelf || !input.hasNext()) {//treat self as a table
        builder.scanNestedTable(context.getCurrentPath());
        if (isNested) {
          RelOptTable table = catalogReader
              .getTableFromPath(pathWalker.getAbsolutePath());
          pullupColumns = IntStream.range(0, table.getKeys().get(0).asSet().size())
              .mapToObj(i -> new PullupColumn(
                  String.format("%spk%d$%s",
                      ReservedName.SYSTEM_HIDDEN_PREFIX, uniquePkId
                          .incrementAndGet(),
                      table.getRowType().getFieldList().get(i).getName()),
                  String.format("%spk%d$%s",
                      ReservedName.SYSTEM_HIDDEN_PREFIX, i + 1,
                      table.getRowType().getFieldList().get(i).getName())
              ))
              .collect(Collectors.toList());
        }
      } else { //treat self as a parameterized binding to the next function
        item = input.next();
        String nextIdentifier = getIdentifier(item)
            .orElseThrow(() -> new RuntimeException("Subqueries are not yet implemented"));
        pathWalker.walk(nextIdentifier);

        SqlUserDefinedTableFunction fnc = catalogReader.getTableFunction(pathWalker.getAbsolutePath());
        List<SqlNode> args = rewriteArgs(ReservedName.SELF_IDENTIFIER.getCanonical(),
            fnc.getFunction(), context.materializeSelf);

        builder.scanFunction(fnc, args);
      }
    } else {
      throw new RuntimeException("Unknown table: " + item);
    }

    while (input.hasNext()) {
      item = input.next();
      String nextIdentifier = getIdentifier(item)
          .orElseThrow(() -> new RuntimeException("Subqueries are not yet implemented"));
      pathWalker.walk(nextIdentifier);

      String alias = builder.getLatestAlias();
      SqlUserDefinedTableFunction fnc = catalogReader.getTableFunction(pathWalker.getPath());
      if (fnc == null) {
        builder.scanNestedTable(pathWalker.getPath());
      } else {
        List<SqlNode> args = rewriteArgs(alias, fnc.getFunction(), context.materializeSelf);
        builder.scanFunction(fnc, args)
            .joinLateral();
      }
    }

    SqlNode sqlNode = builder.buildAndProjectLast(pullupColumns);

    return new Result(sqlNode, pathWalker.getAbsolutePath(), pullupColumns, List.of(), Optional.empty());
  }

  private List<SqlNode> rewriteArgs(String alias, TableFunction function, boolean materializeSelf) {
    //if arg needs to by a dynamic expression, rewrite.
    List<SqlNode> nodes = new ArrayList<>();
    for (FunctionParameter parameter : function.getParameters()) {
      SqrlFunctionParameter p = (SqrlFunctionParameter) parameter;
      SqlIdentifier identifier = new SqlIdentifier(List.of(alias, p.getName()),
          SqlParserPos.ZERO);
      SqlNode rewritten = materializeSelf
          ? identifier
          : rewriteToDynamicParam(identifier);
      nodes.add(rewritten);
    }
    return nodes;
  }

  public SqlNode rewriteToDynamicParam(SqlIdentifier id) {
    //if self, check if param list, if not create one
    if (!isSelfField(id.names)) {
      return id;
    }

    for (FunctionParameter p : params) {
      if (paramMapping.containsKey(p)) {
        return paramMapping.get(p);
      }
    }

    RelDataType anyType = typeFactory.createSqlType(SqlTypeName.ANY);
    SqrlFunctionParameter functionParameter = new SqrlFunctionParameter(id.names.get(1),
        Optional.empty(), SqlDataTypeSpecBuilder
        .create(anyType), params.size(), anyType,
        true);
    params.add(functionParameter);
    SqlDynamicParam param = new SqlDynamicParam(functionParameter.getOrdinal(),
        id.getParserPosition());
    paramMapping.put(functionParameter, param);

    return param;
  }

  private Optional<String> getIdentifier(SqlNode item) {
    if (item instanceof SqlIdentifier) {
      return Optional.of(((SqlIdentifier) item).getSimple());
    } else if (item instanceof SqlCall) {
      return Optional.of(((SqlCall) item).getOperator().getName());
    }

    return Optional.empty();
  }

  @Override
  public Result visitJoin(SqlJoin call, Context context) {
    //Check if we should skip the lhs, if it's self and we don't materialize and there is no condition
    if (isSelfTable(call.getLeft())
        && !context.isMaterializeSelf()) {
      Optional<SqlNode> condition = Optional.ofNullable(call.getCondition());
      Result result = SqlNodeVisitor.accept(this, appendAliasIfRequired(call.getRight()), context);
      return new Result(result.sqlNode, result.currentPath, result.pullupColumns, result.tableReferences, condition);
    }

    Result leftNode = SqlNodeVisitor.accept(this,
        appendAliasIfRequired(call.getLeft()), context);
    Context context1 = new Context(context.materializeSelf, leftNode.currentPath, context.aliasPathMap, false,
        false, false);
    Result rightNode = SqlNodeVisitor.accept(this,
        appendAliasIfRequired(call.getRight()), context1);

    SqlNode join = new SqlJoinBuilder(call)
        .rewriteExpressions(new WalkExpressions(context))
        .setLeft(leftNode.getSqlNode())
        .setRight(rightNode.getSqlNode())
        .lateral()
        .build();

    return new Result(join, rightNode.getCurrentPath(), leftNode.pullupColumns, List.of(),
        leftNode.getCondition().or(rightNode::getCondition));
  }

  @Override
  public Result visitSetOperation(SqlCall node, Context context) {
    final List<Result> operandResults = node.getOperandList().stream()
        .map(o -> SqlNodeVisitor.accept(this, o, context))
        .collect(Collectors.toList());

    Optional<SqlNode> condition = operandResults.stream()
        .map(r -> r.condition)
        .flatMap(Optional::stream)
        .findAny();
    if (condition.isPresent()) {
      throw new RuntimeException("Trailing condition abandoned in rewriting");
    }

    Optional<PullupColumn> pullupColumn = operandResults.stream()
        .map(Result::getPullupColumns)
        .flatMap(Collection::stream)
        .findAny();
    if (pullupColumn.isPresent()) {
      throw new RuntimeException("Primary key columns not pulled up in rewriting");
    }

    List<List<String>> tableReferences = operandResults.stream()
        .map(o -> o.tableReferences)
        .flatMap(Collection::stream)
        .collect(Collectors.toList());

    List<SqlNode> operands = operandResults.stream()
        .map(Result::getSqlNode)
        .collect(Collectors.toList());

    SqlCall call = new SqlCallBuilder(node)
        .setOperands(operands)
        .build();

    return new Result(call,
        List.of(),
        List.of(),
        tableReferences,
        Optional.empty());
  }


  @Value
  public static class PullupColumn {

    String columnName;
    String displayName;
  }

  @AllArgsConstructor
  public class WalkExpressions extends SqlShuttle {

    Context context;

    @Override
    public SqlNode visit(SqlCall call) {
      if (call.getKind() == SqlKind.SELECT) {
        SqrlToSql sqrlToSql = new SqrlToSql(typeFactory, catalogReader, nameUtil, operatorTable, dynamicParam,
            uniquePkId, parameters, List.of(), Map.of());
        Result result = sqrlToSql.rewrite(call, false, context.currentPath);

        return result.getSqlNode();
      }

      return super.visit(call);
    }

    @Override
    public SqlNode visit(SqlIdentifier id) {
      if (dynamicParam.get(id) != null) {
        return dynamicParam.get(id);
      }

      Preconditions.checkState(!isVariable(id.names), "Found variable when expecting one.");
      return super.visit(id);
    }
  }

  @Value
  public static class Result {
    SqlNode sqlNode;
    List<String> currentPath;
    List<PullupColumn> pullupColumns;
    List<List<String>> tableReferences;
    Optional<SqlNode> condition;
  }

  @Value
  public static class Context {

    //unbound replaces @ with system args, bound expands @ to table.
    boolean materializeSelf;
    List<String> currentPath;
    Map<String, List<String>> aliasPathMap;
    public boolean isAggregating;
    public boolean isNested;
    public boolean isLimit;

    public void addAlias(String alias, List<String> currentPath) {
      aliasPathMap.put(alias, currentPath);
    }

    public boolean hasAlias(String alias) {
      return aliasPathMap.containsKey(alias);
    }

    public List<String> getAliasPath(String alias) {
      return new ArrayList<>(getAliasPathMap().get(alias));
    }
  }
}