package ai.datasqrl.plan.local.transpile;

import ai.datasqrl.plan.calcite.util.SqlNodeUtil;
import ai.datasqrl.plan.local.transpile.AnalyzeStatement.AbsoluteResolvedTable;
import ai.datasqrl.plan.local.transpile.AnalyzeStatement.Analysis;
import ai.datasqrl.plan.local.transpile.AnalyzeStatement.RelativeResolvedTable;
import ai.datasqrl.plan.local.transpile.AnalyzeStatement.ResolvedTable;
import ai.datasqrl.plan.local.transpile.AnalyzeStatement.SingleTable;
import ai.datasqrl.plan.local.transpile.AnalyzeStatement.VirtualResolvedTable;
import ai.datasqrl.schema.SQRLTable;
import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Stack;
import lombok.Value;
import org.apache.calcite.sql.JoinConditionType;
import org.apache.calcite.sql.JoinType;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.util.SqlShuttle;

/**
 * Orders.entries.customer = SELECT __a1.customerid
 *                           FROM _.entries.parent p
 *                           LEFT JOIN e.parent AS __a1;
 * ->
 * Orders.entries.customer = SELECT __a1.customerid
 *                           FROM (_.entries AS g1 JOIN g1.parent AS p)
 *                           LEFT JOIN e.parent AS __a1;
 */
public class FlattenTablePaths extends SqlShuttle {
  private final Analysis analysis;

  public FlattenTablePaths(Analysis analysis) {
    this.analysis = analysis;
  }

  public SqlNode accept(SqlNode node) {
    SqlNode result = node.accept(this);;
    return result;
  }

  public static void addJoinCondition(SqlJoin join, SqlNode condition) {
    if (join.getCondition() == null) {
      join.setOperand(5, condition);
    } else {
      join.setOperand(5, SqlNodeUtil.and(join.getCondition(), condition));
    }
    join.setOperand(4, JoinConditionType.ON.symbol(SqlParserPos.ZERO));
  }

  @Override
  public SqlNode visit(SqlCall call) {
    switch (call.getKind()) {
      case JOIN:
        return super.visit(call);
      case AS:
        //When we rewrite paths, we turn a left tree into a bushy tree and we'll need to add the
        // alias to the last table. So If we do that, we need to remove this alias or calcite
        // will end up wrapping it in a subquery. This is for aesthetics so remove it if it causes
        // problems.
        if (analysis.getTableIdentifiers().get(call.getOperandList().get(0)) != null) {
          return call.getOperandList().get(0).accept(this);
        }


    }

    return super.visit(call);
  }

  @Override
  public SqlNode visit(SqlIdentifier id) {
    if (analysis.getTableIdentifiers().get(id) != null) {
      return expandTable(id);
    }
    return super.visit(id);
  }

  @Value
  class ExpandedTable {
    SqlNode table;
    Optional<SqlNode> pullupCondition;
  }

  int idx = 0;

  private SqlNode expandTable(SqlIdentifier id) {
    String finalAlias = analysis.tableAlias.get(id);
    List<String> suffix;

    SqlIdentifier first;
    ResolvedTable resolve = analysis.tableIdentifiers.get(id);
    if (resolve instanceof SingleTable || resolve instanceof VirtualResolvedTable) {
      suffix = List.of();
      first = new SqlIdentifier(List.of(
          id.names.get(0)
      ), SqlParserPos.ZERO);
    } else if (resolve instanceof AbsoluteResolvedTable) {
      suffix = id.names.subList(1, id.names.size());
      first = new SqlIdentifier(List.of(
          id.names.get(0)
      ), SqlParserPos.ZERO);
    } else if (resolve instanceof RelativeResolvedTable){
      first = new SqlIdentifier(List.of( id.names.get(0),
          id.names.get(1)
      ), SqlParserPos.ZERO);
      if ( id.names.size() > 2) {
        suffix =  id.names.subList(2, id.names.size());
      } else {
        suffix = List.of();
      }

    } else {
      throw new RuntimeException("");
    }

    String firstAlias = "_g" + (++idx);
    SqlNode n =
        SqlStdOperatorTable.AS.createCall(SqlParserPos.ZERO,
            first,
            new SqlIdentifier(suffix.size() > 0 ? firstAlias: finalAlias,
                SqlParserPos.ZERO));
    if (suffix.size() >= 1) {
      String currentAlias = "_g" + (idx);

      for (int i = 0; i < suffix.size(); i++) {
        String nextAlias = i == suffix.size() - 1 ? finalAlias : "_g" + (++idx);
        n = new SqlJoin(
            SqlParserPos.ZERO,
            n,
            SqlLiteral.createBoolean(false, SqlParserPos.ZERO),
            JoinType.DEFAULT.symbol(SqlParserPos.ZERO),
            SqlStdOperatorTable.AS.createCall(SqlParserPos.ZERO,
                new SqlIdentifier(List.of(currentAlias, suffix.get(i)), SqlParserPos.ZERO),
                new SqlIdentifier(nextAlias, SqlParserPos.ZERO)),
            JoinConditionType.NONE.symbol(SqlParserPos.ZERO),
            null
        );
        currentAlias = nextAlias;
      }
    }

    return n;
  }
}
