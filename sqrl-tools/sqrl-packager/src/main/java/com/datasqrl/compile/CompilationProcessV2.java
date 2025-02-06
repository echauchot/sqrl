package com.datasqrl.compile;

import com.datasqrl.actions.CreateDatabaseQueries;
import com.datasqrl.actions.GraphqlPostplanHook;
import com.datasqrl.actions.InferGraphqlSchema;
import com.datasqrl.actions.WriteDag;
import com.datasqrl.config.BuildPath;
import com.datasqrl.config.GraphqlSourceFactory;
import com.datasqrl.engine.EnginePhysicalPlan;
import com.datasqrl.engine.PhysicalPlan;
import com.datasqrl.engine.PhysicalPlanner;
import com.datasqrl.engine.pipeline.ExecutionPipeline;
import com.datasqrl.plan.queries.APISource;
import com.datasqrl.v2.dag.DAGBuilder;
import com.datasqrl.v2.dag.DAGPlanner;
import com.datasqrl.v2.dag.PipelineDAG;
import com.datasqrl.v2.SqlScriptPlanner;
import com.datasqrl.v2.Sqrl2FlinkSQLTranslator;
import com.datasqrl.graphql.APIConnectorManagerImpl;
import com.datasqrl.graphql.inference.GraphQLMutationExtraction;
import com.datasqrl.loaders.ModuleLoader;
import com.datasqrl.plan.MainScript;
import com.datasqrl.plan.validate.ExecutionGoal;
import com.datasqrl.v2.dag.plan.ServerStagePlan;
import com.datasqrl.v2.graphql.generate.GraphqlSchemaFactory2;
import com.google.inject.Inject;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import graphql.schema.GraphQLSchema;
import lombok.AllArgsConstructor;
import org.apache.commons.lang3.tuple.Pair;

@AllArgsConstructor(onConstructor_=@Inject)
public class CompilationProcessV2 {

  private final SqlScriptPlanner planner;
  private final ModuleLoader moduleLoader;
  private final APIConnectorManagerImpl apiConnectorManager;
  private final DAGPlanner dagPlanner;
  private final BuildPath buildPath;
  private final MainScript mainScript;
  private final PhysicalPlanner physicalPlanner;
  private final GraphqlPostplanHook graphqlPostplanHook;
  private final CreateDatabaseQueries createDatabaseQueries;
  private final InferGraphqlSchema inferGraphqlSchema;
  private final WriteDag writeDeploymentArtifactsHook;
  //  private final FlinkSqlGenerator flinkSqlGenerator;
  private final GraphqlSourceFactory graphqlSourceFactory;
  private final ExecutionGoal executionGoal;
  private final GraphQLMutationExtraction graphQLMutationExtraction;
  private final ExecutionPipeline pipeline;
  private final TestPlanner testPlanner;
  private final GraphqlSchemaFactory2 graphqlSchemaFactory;

  public Pair<PhysicalPlan, TestPlan> executeCompilation(Optional<Path> testsPath) {

    Sqrl2FlinkSQLTranslator environment = new Sqrl2FlinkSQLTranslator(buildPath);
    planner.planMain(mainScript, environment);
    DAGBuilder dagBuilder = planner.getDagBuilder();
    PipelineDAG dag = dagPlanner.optimize(dagBuilder.getDag());
//    System.out.println(dag);

    //TODO merge my work on dealing with user schema and merge of schemas
    // TODO merge with Matthias latest work.
    List<EnginePhysicalPlan> plans = dagPlanner.assemble(dag, environment);
    final ServerStagePlan serverStagePlan =
        (ServerStagePlan)
            plans.stream()
                .filter(plan -> plan instanceof ServerStagePlan)
                .collect(Collectors.toList())
                .get(0); // there is only one server plan
    final Optional<GraphQLSchema> graphQLSchema = graphqlSchemaFactory.generate(executionGoal, serverStagePlan);


    return null;
//    postcompileHooks();
//    Optional<APISource> source = inferencePostcompileHook.run(testsPath);
//    SqrlDAG dag = dagPlanner.planLogical();
//    PhysicalDAGPlan dagPlan = dagPlanner.planPhysical(dag);
//
//    PhysicalPlan physicalPlan = physicalPlanner.plan(dagPlan);
//    graphqlPostplanHook.updatePlan(source, physicalPlan);
//
//    //create test artifact
//    TestPlan testPlan;
//    if (source.isPresent() && executionGoal == ExecutionGoal.TEST) {
//      testPlan = testPlanner.generateTestPlan(source.get(), testsPath);
//    } else {
//      testPlan = null;
//    }
//    writeDeploymentArtifactsHook.run(dag);
//    return Pair.of(physicalPlan, testPlan);
  }

  private void postcompileHooks() {
    createDatabaseQueries.run();
  }
}

