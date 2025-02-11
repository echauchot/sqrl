package com.datasqrl.v2;


import static com.datasqrl.v2.parser.ParsePosUtil.convertPosition;
import static com.datasqrl.v2.parser.StatementParserException.checkFatal;

import com.datasqrl.canonicalizer.Name;
import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.config.EngineType;
import com.datasqrl.config.PackageJson;
import com.datasqrl.config.SystemBuiltInConnectors;
import com.datasqrl.engine.log.LogEngine;
import com.datasqrl.engine.pipeline.ExecutionPipeline;
import com.datasqrl.engine.pipeline.ExecutionStage;
import com.datasqrl.error.CollectedException;
import com.datasqrl.error.ErrorCode;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.error.ErrorLabel;
import com.datasqrl.error.ErrorLocation.FileLocation;
import com.datasqrl.v2.Sqrl2FlinkSQLTranslator.MutationBuilder;
import com.datasqrl.v2.analyzer.TableAnalysis;
import com.datasqrl.v2.analyzer.cost.SimpleCostAnalysisModel;
import com.datasqrl.v2.dag.DAGBuilder;
import com.datasqrl.v2.dag.nodes.ExportNode;
import com.datasqrl.v2.dag.nodes.PipelineNode;
import com.datasqrl.v2.dag.nodes.TableFunctionNode;
import com.datasqrl.v2.dag.nodes.TableNode;
import com.datasqrl.v2.hint.ExecHint;
import com.datasqrl.v2.hint.PlannerHints;
import com.datasqrl.v2.hint.TestHint;
import com.datasqrl.v2.parser.AccessModifier;
import com.datasqrl.v2.parser.FlinkSQLStatement;
import com.datasqrl.v2.parser.ParsedObject;
import com.datasqrl.v2.parser.SQLStatement;
import com.datasqrl.v2.parser.SqlScriptStatementSplitter;
import com.datasqrl.v2.parser.SqrlAddColumnStatement;
import com.datasqrl.v2.parser.SqrlCreateTableStatement;
import com.datasqrl.v2.parser.SqrlDefinition;
import com.datasqrl.v2.parser.SqrlExportStatement;
import com.datasqrl.v2.parser.SqrlImportStatement;
import com.datasqrl.v2.parser.SqrlStatement;
import com.datasqrl.v2.parser.SqrlStatementParser;
import com.datasqrl.v2.parser.SqrlTableDefinition;
import com.datasqrl.v2.parser.SqrlTableFunctionStatement;
import com.datasqrl.v2.parser.SqrlTableFunctionStatement.ParsedArgument;
import com.datasqrl.v2.parser.StackableStatement;
import com.datasqrl.v2.parser.StatementParserException;
import com.datasqrl.v2.tables.AccessVisibility;
import com.datasqrl.v2.dag.plan.MutationQuery;
import com.datasqrl.v2.tables.SqrlTableFunction;
import com.datasqrl.function.FlinkUdfNsObject;
import com.datasqrl.io.schema.flexible.converters.SchemaToRelDataTypeFactory;
import com.datasqrl.loaders.FlinkTableNamespaceObject;
import com.datasqrl.loaders.FlinkTableNamespaceObject.FlinkTable;
import com.datasqrl.loaders.ModuleLoader;
import com.datasqrl.loaders.ScriptSqrlModule.ScriptNamespaceObject;
import com.datasqrl.module.NamespaceObject;
import com.datasqrl.module.SqrlModule;
import com.datasqrl.plan.MainScript;
import com.datasqrl.plan.global.StageAnalysis;
import com.datasqrl.plan.global.StageAnalysis.Cost;
import com.datasqrl.plan.global.StageAnalysis.MissingCapability;
import com.datasqrl.plan.rules.EngineCapability;
import com.datasqrl.plan.rules.EngineCapability.Feature;
import com.datasqrl.plan.table.RelDataTypeTableSchema;
import com.datasqrl.plan.validate.ExecutionGoal;
import com.datasqrl.util.SqlNameUtil;
import com.google.common.base.Preconditions;
import com.google.inject.Inject;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import lombok.Getter;
import lombok.Value;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.runtime.CalciteContextException;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.flink.sql.parser.ddl.SqlAlterTable;
import org.apache.flink.sql.parser.ddl.SqlAlterView;
import org.apache.flink.sql.parser.ddl.SqlAlterViewAs;
import org.apache.flink.sql.parser.ddl.SqlCreateTable;
import org.apache.flink.sql.parser.ddl.SqlCreateView;
import org.apache.flink.sql.parser.ddl.SqlDropTable;
import org.apache.flink.sql.parser.ddl.SqlDropView;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.functions.UserDefinedFunction;

public class SqlScriptPlanner {

  public static final String EXPORT_SUFFIX = "_ex";
  public static final String  ACCESS_FUNCTION_SUFFIX = "__access";


  private final ErrorCollector errorCollector;

  private final ModuleLoader moduleLoader;
  private final SqrlStatementParser sqrlParser;
  private final PackageJson packageJson;
  private final ExecutionPipeline pipeline;
  private final ExecutionGoal executionGoal;

  @Getter
  private final DAGBuilder dagBuilder;
  private final ExecutionStage streamStage;
  private final List<ExecutionStage> tableStages;
  private final List<ExecutionStage> queryStages;
  private final List<ExecutionStage> subscriptionStages;
  private final AtomicInteger exportTableCounter = new AtomicInteger(0);


  @Inject
  public SqlScriptPlanner(ErrorCollector errorCollector, ModuleLoader moduleLoader,
      SqrlStatementParser sqrlParser, PackageJson packageJson,
      ExecutionPipeline pipeline, ExecutionGoal executionGoal) {
    this.errorCollector = errorCollector;
    this.moduleLoader = moduleLoader;
    this.sqrlParser = sqrlParser;
    this.packageJson = packageJson;
    this.pipeline = pipeline;
    this.executionGoal = executionGoal;

    this.dagBuilder = new DAGBuilder();
    Optional<ExecutionStage> streamStage = pipeline.getStageByType(EngineType.STREAMS);
    errorCollector.checkFatal(streamStage.isPresent(), "Need to configure a stream execution engine");
    this.streamStage = streamStage.get();
    /* to support server execution, we add server_query to tables and query Stages
    and server_subscribe to tables and subscription stages.
     */
    this.tableStages = pipeline.getStages().stream().filter(stage -> stage.getType().isDataStore() || stage.getType()==EngineType.STREAMS).collect(
        Collectors.toList());
    this.queryStages = pipeline.getStages().stream().filter(stage -> stage.getType()==EngineType.DATABASE).collect(
        Collectors.toList());
    this.subscriptionStages = pipeline.getStages().stream().filter(stage -> stage.getType()==EngineType.LOG).collect(
        Collectors.toList());
  }

  public void planMain(MainScript mainScript, Sqrl2FlinkSQLTranslator sqrlEnv) {
    ErrorCollector scriptErrors = errorCollector.withScript(mainScript.getPath(), mainScript.getContent());
    List<ParsedObject<SQLStatement>> statements = sqrlParser.parseScript(mainScript.getContent(), scriptErrors);
    List<StackableStatement> statementStack = new ArrayList<>();
    for (ParsedObject<SQLStatement> statement : statements) {
      ErrorCollector lineErrors = scriptErrors.atFile(statement.getFileLocation());
      SQLStatement sqlStatement = statement.get();
      try {
        planStatement(sqlStatement, statementStack, sqrlEnv, lineErrors);
      } catch (CollectedException e) {
        throw e;
      } catch (Exception e) {
        //Map errors from the Flink parser/planner by adjusting the line numbers
        if (e instanceof org.apache.flink.table.api.SqlParserException) {
          e = (Exception)e.getCause();
        }
        FileLocation location = null;
        String message = null;
        if (e instanceof SqlParseException) {
          location = convertPosition(((SqlParseException) e).getPos());
          message = e.getMessage();
          message = message.replaceAll(" at line \\d*, column \\d*", ""); //remove line number from message
        }
        if (e instanceof CalciteContextException) {
          CalciteContextException calciteException = (CalciteContextException) e;
          location = new FileLocation(calciteException.getPosLine(), calciteException.getPosColumn());
          message = calciteException.getMessage();
          message = message.replaceAll("From line \\d*, column \\d* to line \\d*, column \\d*: ", ""); //remove line number from message
        }
        if (location != null) {
          location = sqlStatement.mapSqlLocation(location);
          e.printStackTrace();
          scriptErrors.atFile(statement.getFileLocation().add(location)).fatal(message);
        }

        //Print stack trace for unknown exceptions
        if (e.getMessage() == null || e instanceof IllegalStateException
            || e instanceof NullPointerException) {
          e.printStackTrace();
        }
        //Use registered error handlers
        throw lineErrors.handle(e);
      }
      if (sqlStatement instanceof StackableStatement) {
        StackableStatement stackableStatement = (StackableStatement) sqlStatement;
        if (stackableStatement.isRoot()) statementStack = new ArrayList<>();
        statementStack.add(stackableStatement);
      } else {
        statementStack = new ArrayList<>();
      }
    }
  }

  private void planStatement(SQLStatement stmt, List<StackableStatement> statementStack, Sqrl2FlinkSQLTranslator sqrlEnv, ErrorCollector errors) throws SqlParseException {
    //Process hints
    PlannerHints hints = PlannerHints.EMPTY;
    if (stmt instanceof SqrlStatement) {
      hints = PlannerHints.fromHints(((SqrlStatement)stmt).getComments());
    }
    if (stmt instanceof SqrlImportStatement) {
      addImport((SqrlImportStatement) stmt, sqrlEnv, errors);
    } else if (stmt instanceof SqrlExportStatement) {
      addExport((SqrlExportStatement) stmt, sqrlEnv);
    } else if (stmt instanceof SqrlCreateTableStatement) {
      addSourceToDag(sqrlEnv.createTable(((SqrlCreateTableStatement) stmt).toSql(), getLogEngineBuilder()), sqrlEnv);
    } else if (stmt instanceof SqrlDefinition) {
      SqrlDefinition sqrlDef = (SqrlDefinition) stmt;
      AccessModifier access = sqrlDef.getAccess();
      NamePath tablePath = sqrlDef.getPath();
      if (sqrlDef instanceof SqrlAddColumnStatement) {
        tablePath = sqrlDef.getPath().popLast();
        StatementParserException.checkFatal(!statementStack.isEmpty() &&
                statementStack.get(0) instanceof SqrlTableDefinition && ((SqrlTableDefinition)statementStack.get(0)).getPath().equals(tablePath),
            sqrlDef.getTableName().getFileLocation(), ErrorCode.INVALID_SQRL_ADD_COLUMN,
            "Column expression must directly follow the definition of table [%s]", tablePath);
        access = ((SqrlTableDefinition)statementStack.get(0)).getAccess();
      }
      boolean nameIsHidden = tablePath.getLast().isHidden();
      //Ignore test tables (that are not queries) when we are not running tests
      if (nameIsHidden && (executionGoal!=ExecutionGoal.TEST || !hints.isTest()) && !hints.isWorkload()) {
        //Test tables should not have access unless we are running tests or they are also workloads
        access = AccessModifier.NONE;
      }
      boolean isHidden = (nameIsHidden || hints.isWorkload()) &&
          !(hints.isTest() && executionGoal==ExecutionGoal.TEST);


      String originalSql = sqrlDef.toSql(sqrlEnv, statementStack);
      //Relationships and Table functions require special handling
      if (sqrlDef instanceof SqrlTableFunctionStatement) {
        SqrlTableFunctionStatement tblFctStmt = (SqrlTableFunctionStatement) sqrlDef;
        ObjectIdentifier identifier = SqlNameUtil.toIdentifier(tblFctStmt.getPath().getFirst());
        List<ParsedArgument> arguments = tblFctStmt.getArguments();
        if (tblFctStmt.isRelationship()) { // Resolve arguments against parent table
          Optional<PipelineNode> parentNode = dagBuilder.getNode(identifier);
          checkFatal(parentNode.isPresent(), sqrlDef.getTableName().getFileLocation(), ErrorCode.INVALID_TABLE_FUNCTION_ARGUMENTS,
              "Could not find parent table for relationship: %s", tblFctStmt.getPath().getFirst());
          checkFatal(parentNode.get() instanceof TableNode, sqrlDef.getTableName().getFileLocation(), ErrorCode.INVALID_TABLE_FUNCTION_ARGUMENTS,
              "Relationships can only be added to tables (not functions): %s [%s]", tblFctStmt.getPath().getFirst(), parentNode.get().getClass());
          identifier = SqlNameUtil.toIdentifier(Name.system("relationship"));
          TableAnalysis parentTbl = ((TableNode) parentNode.get()).getTableAnalysis();
          arguments = arguments.stream().map(arg -> {
            if (arg.isParentField()) {
              RelDataTypeField field = parentTbl.getRowType().getField(arg.getName().get(), false, false);
              checkFatal(field!=null, arg.getName().getFileLocation(), ErrorLabel.GENERIC,
                  "Could not find field on parent table: %s", arg.getName());
              return arg.withResolvedType(field.getType()).withName(
                  new ParsedObject<String>(field.getName(), arg.getName().getFileLocation()));
            } else {
              return arg;
            }
          }).collect(Collectors.toList());
        }

        var fctBuilder = sqrlEnv.resolveSqrlTableFunction(identifier, originalSql, arguments, tblFctStmt.getArgIndexMap(), hints, errors);
        fctBuilder.fullPath(tblFctStmt.getPath());
        AccessVisibility visibility = new AccessVisibility(access, hints.isTest(), tblFctStmt.isRelationship(), isHidden);
        fctBuilder.visibility(visibility);
        SqrlTableFunction fct = fctBuilder.build();
        addFunctionToDag(fct, hints);
        if (!fct.getVisibility().isAccessOnly()) {
          sqrlEnv.registerSqrlTableFunction(fct);
        }
      } else {
        AccessVisibility visibility = new AccessVisibility(access, hints.isTest(), true, isHidden);
        addTableToDag(sqrlEnv.addView(originalSql, hints, errors), hints, visibility, sqrlEnv);
      }
    } else if (stmt instanceof FlinkSQLStatement) { //Some other Flink table statement we pass right through
      FlinkSQLStatement flinkStmt = (FlinkSQLStatement) stmt;
      SqlNode node = sqrlEnv.parseSQL(flinkStmt.getSql().get());
      if (node instanceof SqlCreateView || node instanceof SqlAlterViewAs) {
        //plan like other definitions from above
        AccessVisibility visibility = new AccessVisibility(AccessModifier.QUERY, false, true, false);
        addTableToDag(sqrlEnv.addView(flinkStmt.getSql().get(), hints, errors), hints, visibility, sqrlEnv);
      } else if (node instanceof SqlCreateTable) {
        addSourceToDag(sqrlEnv.createTable(flinkStmt.getSql().get(), getLogEngineBuilder()), sqrlEnv);
      } else if (node instanceof SqlAlterTable || node instanceof SqlAlterView) {
        errors.fatal("Renaming or altering tables is not supported. Rename them directly in the script or IMPORT AS.");
      } else if (node instanceof SqlDropTable || node instanceof SqlDropView) {
        errors.fatal("Removing tables is not supported. The DAG planner automatically removes unused tables.");
      } else {
        //just pass through
        sqrlEnv.executeSQL(flinkStmt.getSql().get());
      }
    }
  }

  public static final Name STAR = Name.system("*");

  private void addImport(SqrlImportStatement importStmt, Sqrl2FlinkSQLTranslator sqrlEnv, ErrorCollector errors) {
    NamePath path = importStmt.getPackageIdentifier().get();
    boolean isStar = path.getLast().equals(STAR);

    //Alias
    Optional<Name> alias = Optional.empty();
    if (importStmt.getAlias().isPresent()) {
      NamePath aliasPath = importStmt.getAlias().get();
      checkFatal(aliasPath.size()==1, ErrorCode.INVALID_IMPORT, "Invalid table name - paths not supported");
      alias = Optional.of(aliasPath.getFirst());
    }

    SqrlModule module = moduleLoader.getModule(path.popLast()).orElse(null);
    checkFatal(module!=null, importStmt.getPackageIdentifier().getFileLocation(), ErrorLabel.GENERIC,
        "Could not find module [%s] at path: [%s]", path, String.join("/", path.toStringList()));

    if (isStar) {
      if (module.getNamespaceObjects().isEmpty()) {
        errors.warn("Module is empty: %s", path);
      }
      for (NamespaceObject namespaceObject : module.getNamespaceObjects()) {
        //For multiple imports, the alias serves as a prefix.
        addImport(namespaceObject, alias.map(x -> x.append(namespaceObject.getName()).getDisplay()), sqrlEnv);
      }
    } else {
      Optional<NamespaceObject> namespaceObject = module.getNamespaceObject(path.getLast());
      errors.checkFatal(namespaceObject.isPresent(), "Object [%s] not found in module: %s", path.getLast(), path);

      addImport(namespaceObject.get(),
          Optional.of(alias.orElse(path.getLast()).getDisplay()),
          sqrlEnv);
    }
  }

  private void addSourceToDag(TableAnalysis tableAnalysis, Sqrl2FlinkSQLTranslator sqrlEnv) {
    Preconditions.checkArgument(tableAnalysis.getFromTables().size()==1);
    TableAnalysis source = (TableAnalysis) tableAnalysis.getFromTables().get(0);
    Preconditions.checkArgument(source.isSourceOrSink());
    TableNode sourceNode = new TableNode(source, getSourceSinkStageAnalysis());
    dagBuilder.add(sourceNode);
    boolean isHidden = Name.system(tableAnalysis.getIdentifier().getObjectName()).isHidden();
    AccessVisibility visibility = new AccessVisibility(isHidden?AccessModifier.NONE:AccessModifier.QUERY, false, true,
        isHidden);
    addTableToDag(tableAnalysis, PlannerHints.EMPTY, visibility, sqrlEnv);
  }

  private Map<ExecutionStage, StageAnalysis> getSourceSinkStageAnalysis() {
    return Map.of(streamStage,
        new Cost(streamStage, SimpleCostAnalysisModel.ofSourceSink(), true));
  }

  private Map<ExecutionStage, StageAnalysis> getStageAnalysis(
      TableAnalysis tableAnalysis, List<ExecutionStage> availableStages) {
    Map<ExecutionStage, StageAnalysis> stageAnalysis = new HashMap<>();
    for (ExecutionStage executionStage : availableStages) {
      List<EngineCapability> unsupported = tableAnalysis.getRequiredCapabilities().stream().filter(capability -> {
        if (capability instanceof EngineCapability.Feature) {
          return !executionStage.supportsFeature(((Feature) capability).getFeature());
        } else if (capability instanceof EngineCapability.Function) {
          return !executionStage.supportsFunction(
              ((EngineCapability.Function) capability).getFunction());
        } else {
          throw new UnsupportedOperationException(capability.getName());
        }
      }).collect(Collectors.toList());
      if (unsupported.isEmpty()) {
        stageAnalysis.put(executionStage,
            new Cost(executionStage,
                SimpleCostAnalysisModel.of(executionStage, tableAnalysis),
                true));
      } else {
        stageAnalysis.put(executionStage, new MissingCapability(executionStage, unsupported));
      }
    }
    return stageAnalysis;
  }

  private List<ExecutionStage> determineStages(List<ExecutionStage> availableStages, PlannerHints hints) {
    Optional<ExecHint> executionHint = hints.getHint(ExecHint.class);
    if (hints.isTest() || hints.isWorkload()) {
      //Tests and hints always get executed in the database
      availableStages = availableStages.stream().filter(stage -> stage.getType()==EngineType.DATABASE || stage.getType()==EngineType.SERVER)
          .collect(Collectors.toList());
      if (availableStages.isEmpty()) {
        throw new StatementParserException(ErrorLabel.GENERIC,
            hints.getHint(TestHint.class).get().getSource().getFileLocation(),
            "Could not find suitable database stage to execute tests or workloads: %s", availableStages);
      }
    }
    if (executionHint.isPresent()) {
      var execHint = executionHint.get();
      availableStages = availableStages.stream().filter(stage ->
          execHint.getStageNames().stream().anyMatch(name -> stage.getName().equalsIgnoreCase(name)
              || stage.getEngine().getType().name().equalsIgnoreCase(name))
      ).collect(Collectors.toList());
      if (availableStages.isEmpty()) {
        throw new StatementParserException(ErrorLabel.GENERIC, execHint.getSource().getFileLocation(),
            "Provided execution stages could not be found or are not configured: %s", execHint.getStageNames());
      }
    }
    assert !availableStages.isEmpty();
    return availableStages;
  }

  private List<ExecutionStage> determineViableStages(AccessModifier access) {
    if (access == AccessModifier.QUERY) return queryStages;
    else if (access == AccessModifier.SUBSCRIPTION) return subscriptionStages;
    else return tableStages;
  }

  private void addTableToDag(TableAnalysis tableAnalysis, PlannerHints hints, AccessVisibility visibility,
      Sqrl2FlinkSQLTranslator sqrlEnv) {
    List<ExecutionStage> availableStages = determineStages(tableStages,hints);

    TableNode tableNode = new TableNode(tableAnalysis, getStageAnalysis(tableAnalysis, availableStages));
    dagBuilder.add(tableNode);
    if (visibility.isEndpoint()) { //Add function to scan table as endpoint
      String tableName = tableAnalysis.getIdentifier().getObjectName();
      String fctName = tableName + ACCESS_FUNCTION_SUFFIX;
      String scanViewSql = sqrlEnv.toSqlString(sqrlEnv.createScanView(fctName, tableAnalysis.getIdentifier()));
      //TODO: should we add a default sort if the user didn't specify one to have predictable result sets for testing?
      SqrlTableFunction.SqrlTableFunctionBuilder fctBuilder = sqrlEnv.resolveSqrlTableFunction(SqlNameUtil.toIdentifier(Name.system(fctName)),
          scanViewSql, List.of(), Map.of(), PlannerHints.EMPTY, ErrorCollector.root());
      fctBuilder.fullPath(NamePath.of(tableName));
      fctBuilder.visibility(visibility);
      addFunctionToDag(fctBuilder.build(), hints);
    }
  }

  private void addFunctionToDag(SqrlTableFunction function, PlannerHints hints) {
    List<ExecutionStage> availableStages = determineStages(determineViableStages(function.getVisibility().getAccess()),hints);
    dagBuilder.add(new TableFunctionNode(function, getStageAnalysis(function.getFunctionAnalysis(), availableStages)));
  }


  private void addImport(NamespaceObject nsObject, Optional<String> alias, Sqrl2FlinkSQLTranslator sqrlEnv) {
    if (nsObject instanceof FlinkTableNamespaceObject) {
      //TODO: for a create table statement without options (connector), we manage it internally
      // add pass it to Log engine for augmentation after validating/adding event-id and event-time metadata columns & checking no watermark/partition/constraint is present
      ExternalFlinkTable flinkTable = ExternalFlinkTable.fromNamespaceObject((FlinkTableNamespaceObject) nsObject,
          alias, errorCollector);
      try {
        TableAnalysis tableAnalysis = sqrlEnv.addImport(flinkTable.tableName.getDisplay(), flinkTable.sqlCreateTable,
            flinkTable.schema, getLogEngineBuilder());
        addSourceToDag(tableAnalysis, sqrlEnv);
      } catch (Throwable e) {
        throw flinkTable.errorCollector.handle(e);
      }
    } else if (nsObject instanceof FlinkUdfNsObject) {
      FlinkUdfNsObject fnsObject = (FlinkUdfNsObject) nsObject;
      Preconditions.checkArgument(fnsObject.getFunction() instanceof UserDefinedFunction, "Expected UDF: " + fnsObject.getFunction());
      Class<?> udfClass = fnsObject.getFunction().getClass();
      String name = alias.orElseGet(() -> FlinkUdfNsObject.getFunctionNameFromClass(udfClass).getDisplay());
      sqrlEnv.addUserDefinedFunction(name, udfClass.getName(), false);
    } else if (nsObject instanceof ScriptNamespaceObject) {
      ScriptNamespaceObject scriptObject = (ScriptNamespaceObject) nsObject;
      checkFatal(scriptObject.getTableName().isEmpty(), ErrorLabel.GENERIC, "Cannot import an individual table from SQRL script. Use * to import entire script: %s", scriptObject.getName());
      planMain(scriptObject.getScript(), sqrlEnv);
    } else {
      throw new UnsupportedOperationException("Unexpected object imported: " + nsObject);
    }
  }

  private MutationBuilder getLogEngineBuilder() {
    Optional<ExecutionStage> logStage = pipeline.getStageByType(EngineType.LOG);
    Preconditions.checkArgument(logStage.isPresent());
    LogEngine engine = (LogEngine) logStage.get().getEngine();
    return (tableBuilder, datatype) -> {
      MutationQuery.MutationQueryBuilder mutationBuilder = MutationQuery.builder();
      mutationBuilder.stage(logStage.get());
      mutationBuilder.createTopic(engine.createTable(logStage.get(), tableBuilder.getTableName(), tableBuilder, datatype, Optional.empty()));
      mutationBuilder.name(Name.system(tableBuilder.getTableName()));
      return mutationBuilder;
    };
  }

  private void addExport(SqrlExportStatement exportStmt, Sqrl2FlinkSQLTranslator sqrlEnv) {
    NamePath sinkPath = exportStmt.getPackageIdentifier().get();
    Name sinkName = sinkPath.getLast();
    NamePath tablePath = exportStmt.getTableIdentifier().get();
    Optional<PipelineNode> tableNode = dagBuilder.getNode(SqlNameUtil.toIdentifier(tablePath.getLast()));

    TableNode inputNode = tableNode.orElseThrow(() -> new StatementParserException(ErrorLabel.GENERIC,
        exportStmt.getTableIdentifier().getFileLocation(), "Could not find table: %s",
        tablePath.toString())).unwrap(TableNode.class);

    String exportTableId = sinkName.getDisplay() + EXPORT_SUFFIX + exportTableCounter.incrementAndGet();
    Map<ExecutionStage, StageAnalysis> stageAnalysis = getSourceSinkStageAnalysis();
    ExportNode exportNode;

    Optional<SystemBuiltInConnectors> builtInSink = SystemBuiltInConnectors.forExport(sinkPath.getFirst())
        .filter(x -> sinkPath.size()==2);
    if (builtInSink.isPresent()) {
      SystemBuiltInConnectors connector = builtInSink.get();
      ExecutionStage exportStage;
      if (connector == SystemBuiltInConnectors.LOG_ENGINE) {
        Optional<ExecutionStage> logStage = pipeline.getStageByType(EngineType.LOG);
        errorCollector.checkFatal(logStage.isPresent(), "Cannot export to log since no log engine has been configured");
        exportStage = logStage.get();
      } else {
        String engineName = connector.getName().getCanonical();
        if (connector == SystemBuiltInConnectors.LOGGER) {
          engineName = packageJson.getCompilerConfig().getLogger();
          if (engineName.equalsIgnoreCase("none")) {
            return; //simply ignore
          }
        }
        Optional<ExecutionStage> optStage = pipeline.getStage(engineName);
        errorCollector.checkFatal(optStage.isPresent(), "The configured logger `%s` under 'compiler.logger' is not a configured engine.", engineName);
        exportStage = optStage.get();
      }
      exportNode = new ExportNode(stageAnalysis, sinkPath, Optional.of(exportStage), Optional.empty());
    } else {
      SqrlModule module = moduleLoader.getModule(sinkPath.popLast()).orElse(null);
      checkFatal(module!=null, exportStmt.getPackageIdentifier().getFileLocation(), ErrorLabel.GENERIC,
          "Could not find module [%s] at path: [%s]", sinkPath, String.join("/", sinkPath.toStringList()));

      Optional<NamespaceObject> sinkObj = module.getNamespaceObject(sinkName);
      checkFatal(sinkObj.isPresent(), exportStmt.getPackageIdentifier().getFileLocation(), ErrorLabel.GENERIC,
          "Could not find table [%s] in module [%s]", sinkName, module);
      checkFatal(sinkObj.get() instanceof FlinkTableNamespaceObject, exportStmt.getPackageIdentifier().getFileLocation(), ErrorLabel.GENERIC,
          "Not a valid sink table [%s] in module [%s]", sinkName, module);
      FlinkTableNamespaceObject sinkTable = (FlinkTableNamespaceObject) sinkObj.get();

      ExternalFlinkTable flinkTable = ExternalFlinkTable.fromNamespaceObject(sinkTable,
          Optional.of(sinkName.getDisplay()), errorCollector);
      Optional<RelDataType> schema = flinkTable.schema.or(() -> Optional.of(inputNode.getTableAnalysis().getRowType()));
      try {
        ObjectIdentifier tableId = sqrlEnv.addExternalExport(exportTableId, flinkTable.sqlCreateTable, schema);
        exportNode = new ExportNode(stageAnalysis, sinkPath, Optional.empty(), Optional.of(tableId));
      } catch (Throwable e) {
        throw flinkTable.errorCollector.handle(e);
      }
    }
    dagBuilder.addExport(exportNode, inputNode);
  }

  @Value
  public static class ExternalFlinkTable {

    Name tableName;
    String sqlCreateTable;
    Optional<RelDataType> schema;
    ErrorCollector errorCollector;

    public static ExternalFlinkTable fromNamespaceObject(FlinkTableNamespaceObject nsObject,
        Optional<String> alias, ErrorCollector errorCollector) {
      FlinkTable flinkTable = nsObject.getTable();
      Name tableName = alias.map(Name::system).orElse(flinkTable.getName());

      //Parse SQL
      String tableSql = flinkTable.getFlinkSQL();
      ErrorCollector tableError = errorCollector.withScript(flinkTable.getFlinkSqlFile(), tableSql);
      tableSql = SqlScriptStatementSplitter.formatEndOfSqlFile(tableSql);


      //Schema conversion
      Optional<RelDataType> schema = flinkTable.getSchema().map(tableSchema -> {
        if (tableSchema instanceof RelDataTypeTableSchema) {
          return ((RelDataTypeTableSchema) tableSchema).getRelDataType();
        } else {
          return SchemaToRelDataTypeFactory.load(tableSchema)
              .map(tableSchema, null, tableName, errorCollector);
        }
      });

      return new ExternalFlinkTable(tableName, tableSql, schema, tableError);
    }

  }


}
