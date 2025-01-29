package com.datasqrl.actions;

import com.datasqrl.calcite.SqrlFramework;
import com.datasqrl.canonicalizer.Name;
import com.datasqrl.canonicalizer.NameCanonicalizer;
import com.datasqrl.config.EngineFactory.Type;
import com.datasqrl.config.GraphqlSourceFactory;
import com.datasqrl.engine.pipeline.ExecutionPipeline;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.graphql.APIConnectorManager;
import com.datasqrl.graphql.generate.GraphqlSchemaFactory;
import com.datasqrl.graphql.inference.GraphqlQueryBuilder;
import com.datasqrl.graphql.inference.GraphqlQueryGenerator;
import com.datasqrl.graphql.inference.GraphqlSchemaValidator;
import com.datasqrl.plan.queries.APISource;
import com.datasqrl.plan.queries.APISourceImpl;
import com.datasqrl.plan.queries.APISubscription;
import com.datasqrl.plan.validate.ExecutionGoal;
import com.datasqrl.util.SqlNameUtil;
import com.google.inject.Inject;
import graphql.schema.GraphQLSchema;
import graphql.schema.GraphqlTypeComparatorRegistry;
import graphql.schema.idl.SchemaPrinter;
import java.util.*;
import lombok.AllArgsConstructor;
import lombok.SneakyThrows;

/**
 * Creates new table functions from the GraphQL schema.
 */
@AllArgsConstructor(onConstructor_ = @Inject)
public class InferGraphqlSchema {

  private final ExecutionPipeline pipeline;
  private final SqrlFramework framework;
  private final ErrorCollector errorCollector;
  private final APIConnectorManager apiManager;
  private final GraphqlSourceFactory graphqlSourceFactory;
  private final GraphqlSchemaFactory graphqlSchemaFactory;
  private final ExecutionGoal goal;


  public Optional<APISource> run() {
    if (!isServerStagePresent()) {
      return Optional.empty(); // there is no API exposure, no need for GraphQL schema
    }

    Optional<APISource> apiSourceOpt = getOrInferApiSource();
    return apiSourceOpt.map(apiSource -> {
      ErrorCollector apiErrors = setErrorCollectorSchema(apiSource, errorCollector);
      try {
        validateAndGenerateQueries(apiSource, apiErrors);
      } catch (Exception e) {
        throw apiErrors.handle(e);
      }
      return apiSource;
    });
  }

  @SneakyThrows
  public Optional<String> inferGraphQLSchema() {
    Optional<GraphQLSchema> graphQLSchema = graphqlSchemaFactory.generate(goal);
    final SchemaPrinter.Options opts = createSchemaPrinterOptions();
    return graphQLSchema.map(schema -> new SchemaPrinter(opts).print(schema));
  }

  private ErrorCollector setErrorCollectorSchema(APISource apiSchema, ErrorCollector errorCollector) {
    return errorCollector.withSchema(
        apiSchema.getName().getDisplay(), apiSchema.getSchemaDefinition());
  }

  // Checks if the server stage is present in the execution pipeline
  private boolean isServerStagePresent() {
    return pipeline.getStage(Type.SERVER).isPresent();
  }

  // Creates the SchemaPrinter options
  private SchemaPrinter.Options createSchemaPrinterOptions() {
    return SchemaPrinter.Options.defaultOptions()
        .setComparators(GraphqlTypeComparatorRegistry.AS_IS_REGISTRY)
        .includeDirectives(false);
  }

  // Retrieves the API source, either from the user provided schema or by inferring the schema from the SQRL plan.
  // If in test mode, we ignore the user provided schema and always infer it from the SQRL plan
  private Optional<APISource> getOrInferApiSource() {
    final Optional<APISource> apiSource = graphqlSourceFactory.getUserProvidedSchema();
    if (goal == ExecutionGoal.TEST || apiSource.isEmpty()) {
      return inferGraphQLSchema().map(schemaString -> new APISourceImpl(Name.system("<schema>"), schemaString));
    } else {
      return apiSource;
    }
  }

  // Validates the schema and generates queries and subscriptions
  private void validateAndGenerateQueries(APISource apiSchema, ErrorCollector apiErrors) {
    GraphqlSchemaValidator schemaValidator = new GraphqlSchemaValidator(framework, apiManager);
    schemaValidator.validate(apiSchema, apiErrors);

    GraphqlQueryGenerator queryGenerator = new GraphqlQueryGenerator(
        framework.getCatalogReader().nameMatcher(),
        framework.getSchema(),
        new GraphqlQueryBuilder(framework, apiManager, new SqlNameUtil(NameCanonicalizer.SYSTEM)),
        apiManager
    );

    queryGenerator.walk(apiSchema);

    // Add queries to apiManager
    queryGenerator.getQueries().forEach(apiManager::addQuery);

    // Add subscriptions to apiManager
    final APISource source = apiSchema;
    queryGenerator.getSubscriptions().forEach(subscription ->
        apiManager.addSubscription(
            new APISubscription(subscription.getAbsolutePath().getFirst(), source), subscription)
    );
  }
}
