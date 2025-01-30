package com.datasqrl.graphql.inference;

import static com.datasqrl.graphql.server.TypeDefinitionRegistryUtil.getMutationTypeName;

import com.datasqrl.canonicalizer.Name;
import com.datasqrl.canonicalizer.NameCanonicalizer;
import com.datasqrl.canonicalizer.ReservedName;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.graphql.APIConnectorManager;
import com.datasqrl.graphql.GraphqlSchemaParser;
import com.datasqrl.graphql.visitor.GraphqlSchemaVisitor;
import com.datasqrl.loaders.ModuleLoader;
import com.datasqrl.plan.queries.APIMutation;
import com.datasqrl.plan.queries.APISource;
import com.google.inject.Inject;
import graphql.language.ObjectTypeDefinition;
import graphql.schema.idl.TypeDefinitionRegistry;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.rel.type.RelRecordType;
import org.apache.calcite.sql.type.SqlTypeName;

/**
 * Mutations need to be extracted at the beginning of the compilation because we need to convert
 * them into sources for the SQRL script to plan: if we define a mutation with two field a and b and
 * then define a table in SQRL <b> Table := SELECT a FROM MyMutation </b> we need to know what the
 * schema of MyMutation is to correctly plan the SELECT query and create the MyMutation source.
 * Hence, this needs to happen before. For queries and subscriptions, those are not needed until
 * afterward because they follow what is defined in the SQRL script. In other words defining a
 * subscription or a query on SQRL table does not change the plan.
 */
@Slf4j
@AllArgsConstructor(onConstructor_ = @Inject)
@Getter
public class GraphQLMutationExtraction {
  private final GraphqlSchemaParser schemaParser;
  private final RelDataTypeFactory typeFactory;
  private final NameCanonicalizer canonicalizer;
  private final ModuleLoader moduleLoader;
  private final ErrorCollector errors;
  private final APIConnectorManager connectorManager;

  public void analyze(APISource apiSource) {
    TypeDefinitionRegistry registry = schemaParser.parse(apiSource.getSchemaDefinition());
    ObjectTypeDefinition mutationType = (ObjectTypeDefinition) registry
        .getType(getMutationTypeName(registry))
        .orElse(null);

    if (mutationType == null) {
      log.trace("No mutations in {}", apiSource);
    } else {
      List<RelDataTypeField> types = GraphqlSchemaVisitor.accept(
          new InputFieldToRelDataType(registry, typeFactory, canonicalizer),
          mutationType, registry);

      List<RelDataTypeField> addedFields = appendFields(types);

      for (RelDataTypeField namedType : addedFields) {
        APIMutation apiMutation = new APIMutation(Name.system(namedType.getName()), apiSource,
            namedType.getType(), ReservedName.MUTATION_TIME.getDisplay(), ReservedName.MUTATION_PRIMARY_KEY.getDisplay());
        connectorManager.addMutation(apiMutation);
      }
    }
  }

  private List<RelDataTypeField> appendFields(List<RelDataTypeField> types) {
    List<RelDataTypeField> newFields = new ArrayList<>();
    for (RelDataTypeField field : types) {
      RelRecordType relRecordType = (RelRecordType) field.getType();
      List<RelDataTypeField> fields = new ArrayList<>(relRecordType.getFieldList());
//      fields.add(new RelDataTypeFieldImpl(DEFAULT_EVENT_TIME_NAME, relRecordType.getFieldList().size(),
//          typeFactory.createSqlType(SqlTypeName.TIMESTAMP, 3)));
      fields.add(new RelDataTypeFieldImpl("_uuid", relRecordType.getFieldList().size(),
          typeFactory.createSqlType(SqlTypeName.VARCHAR)));

      newFields.add(new RelDataTypeFieldImpl(field.getName(), field.getIndex(),
          new RelRecordType(relRecordType.getStructKind(), fields, relRecordType.isNullable())));
    }
    return newFields;
  }
}
