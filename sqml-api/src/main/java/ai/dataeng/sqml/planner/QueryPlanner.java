/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package ai.dataeng.sqml.planner;

import ai.dataeng.sqml.analyzer.FieldId;
import ai.dataeng.sqml.analyzer.StatementAnalysis;
import ai.dataeng.sqml.logical4.AggregateOperator;
import ai.dataeng.sqml.logical4.AggregateOperator.Aggregation;
import ai.dataeng.sqml.logical4.FilterOperator;
import ai.dataeng.sqml.logical4.LogicalPlan;
import ai.dataeng.sqml.logical4.LogicalPlan.RowNode;
import ai.dataeng.sqml.logical4.ProjectOperator;
import ai.dataeng.sqml.metadata.Metadata;
import ai.dataeng.sqml.relation.ColumnReferenceExpression;
import ai.dataeng.sqml.relation.VariableReferenceExpression;
import ai.dataeng.sqml.tree.Cast;
import ai.dataeng.sqml.tree.Expression;
import ai.dataeng.sqml.tree.FieldReference;
import ai.dataeng.sqml.tree.FunctionCall;
import ai.dataeng.sqml.tree.Node;
import ai.dataeng.sqml.tree.Query;
import ai.dataeng.sqml.tree.QuerySpecification;
import ai.dataeng.sqml.tree.SymbolReference;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static ai.dataeng.sqml.planner.OriginalExpressionUtils.asSymbolReference;
import static ai.dataeng.sqml.planner.OriginalExpressionUtils.castToRowExpression;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Streams.stream;
import static java.util.Objects.requireNonNull;

class QueryPlanner
{
    private final StatementAnalysis analysis;
    private final PlanVariableAllocator variableAllocator;
    private final RowNodeIdAllocator idAllocator;
    private final Metadata metadata;

    QueryPlanner(
        StatementAnalysis analysis,
            PlanVariableAllocator variableAllocator,
            RowNodeIdAllocator idAllocator,
            Metadata metadata)
    {
        requireNonNull(analysis, "analysis is null");
        requireNonNull(variableAllocator, "variableAllocator is null");
        requireNonNull(idAllocator, "idAllocator is null");
        requireNonNull(metadata, "metadata is null");

        this.analysis = analysis;
        this.variableAllocator = variableAllocator;
        this.idAllocator = idAllocator;
        this.metadata = metadata;
    }

    public RelationPlan plan(Query query)
    {
        PlanBuilder builder = planQueryBody(query);

//        List<Expression> orderBy = analysis.getOrderByExpressions(query);
//        builder = handleSubqueries(builder, query, orderBy);
        List<Expression> outputs = analysis.getOutputExpressions(query);
//        builder = handleSubqueries(builder, query, outputs);
        builder = project(builder, Iterables.concat(/*orderBy,*/ outputs));

//        builder = sort(builder, query);
//        builder = limit(builder, query);
//        builder = project(builder, analysis.getOutputExpressions(query));

        return new RelationPlan(builder.getRoot(), analysis.getScope(query), computeOutputs(builder, analysis.getOutputExpressions(query)));
    }

    public RelationPlan plan(QuerySpecification node)
    {
        PlanBuilder builder = planFrom(node);
//        RelationPlan fromRelationPlan = builder.getRelationPlan();

        builder = filter(builder, analysis.getWhere(node), node);
        builder = aggregate(builder, node);
        builder = filter(builder, analysis.getHaving(node), node);

//        builder = window(builder, node);

        List<Expression> outputs = analysis.getOutputExpressions(node);
//        builder = handleSubqueries(builder, node, outputs);
//
//        if (node.getOrderBy().isPresent()) {
//            if (!analysis.isAggregation(node)) {
//                // ORDER BY requires both output and source fields to be visible if there are no aggregations
//                builder = project(builder, outputs, fromRelationPlan);
//                outputs = toSymbolReferences(computeOutputs(builder, outputs));
//                builder = planBuilderFor(builder, analysis.getScope(node.getOrderBy().get()));
//            }
//            else {
//                // ORDER BY requires output fields, groups and translated aggregations to be visible for queries with aggregation
//                List<Expression> orderByAggregates = analysis.getOrderByAggregates(node.getOrderBy().get());
//                builder = project(builder, Iterables.concat(outputs, orderByAggregates));
//                outputs = toSymbolReferences(computeOutputs(builder, outputs));
//                List<Expression> complexOrderByAggregatesToRemap = orderByAggregates.stream()
//                        .filter(expression -> !analysis.isColumnReference(expression))
//                        .collect(toImmutableList());
//                builder = planBuilderFor(builder, analysis.getScope(node.getOrderBy().get()), complexOrderByAggregatesToRemap);
//            }
//
//            builder = window(builder, node.getOrderBy().get());
//        }

//        List<Expression> orderBy = analysis.getOrderByExpressions(node);
//        builder = handleSubqueries(builder, node, orderBy);
        builder = project(builder, Iterables.concat(/*orderBy,*/ outputs));

//        builder = distinct(builder, node);
//        builder = sort(builder, node);
//        builder = limit(builder, node);
//        builder = project(builder, outputs);

        return new RelationPlan(builder.getRoot(), analysis.getScope(node), computeOutputs(builder, outputs));
    }

    private static List<ColumnReferenceExpression> computeOutputs(PlanBuilder builder, List<Expression> outputExpressions)
    {
        ImmutableList.Builder<ColumnReferenceExpression> outputs = ImmutableList.builder();
        for (Expression expression : outputExpressions) {
            outputs.add(builder.translate(expression));
        }
        return outputs.build();
    }

    private PlanBuilder planQueryBody(Query query)
    {
        RelationPlan relationPlan = new RelationPlanner(analysis, variableAllocator, idAllocator, metadata)
                .process(query.getQueryBody(), null);

        return planBuilderFor(relationPlan);
    }

    private PlanBuilder planFrom(QuerySpecification node)
    {
        RelationPlan relationPlan;

//        if (node.getFrom().isPresent()) {
            relationPlan = new RelationPlanner(analysis, variableAllocator, idAllocator, metadata)
                    .process(node.getFrom(), null);
//        }
//        else {
//            relationPlan = planImplicitTable();
//        }

        return planBuilderFor(relationPlan);
    }
//
//    private PlanBuilder planBuilderFor(PlanBuilder builder, Scope scope, Iterable<? extends Expression> expressionsToRemap)
//    {
//        PlanBuilder newBuilder = planBuilderFor(builder, scope);
//        // We can't deduplicate expressions here because even if two expressions are equal,
//        // the TranslationMap maps sql names to symbols, and any lambda expressions will be
//        // resolved differently since the lambdaDeclarationToVariableMap is identity based.
//        stream(expressionsToRemap)
//                .forEach(expression -> newBuilder.getTranslations().put(expression, builder.translate(expression)));
//        return newBuilder;
//    }
//
//    private PlanBuilder planBuilderFor(PlanBuilder builder, Scope scope)
//    {
//        return planBuilderFor(new RelationPlan(builder.getRoot(), scope, builder.getRoot().getOutputVariables()));
//    }

    private PlanBuilder planBuilderFor(RelationPlan relationPlan)
    {
        TranslationMap translations = new TranslationMap(relationPlan, analysis);

        // Make field->variable mapping from underlying relation plan available for translations
        // This makes it possible to rewrite FieldOrExpressions that reference fields from the FROM clause directly
        translations.setFieldMappings(relationPlan.getFieldMappings());

        return new PlanBuilder(translations, relationPlan.getRoot());
    }
//
//    private RelationPlan planImplicitTable()
//    {
//        Scope scope = Scope.create();
//        return new RelationPlan(
//                new ValuesNode(idAllocator.getNextId(), ImmutableList.of(), ImmutableList.of(ImmutableList.of())),
//                scope,
//                ImmutableList.of());
//    }

    private PlanBuilder filter(PlanBuilder subPlan, Expression predicate, Node node)
    {
        if (predicate == null) {
            return subPlan;
        }

        // rewrite expressions which contain already handled subqueries
//        Expression rewrittenBeforeSubqueries = subPlan.rewrite(predicate);
//        subPlan = subqueryPlanner.handleSubqueries(subPlan, rewrittenBeforeSubqueries, node);
        Expression rewrittenAfterSubqueries = subPlan.rewrite(predicate);

        return subPlan.withNewRoot(new FilterOperator((RowNode)subPlan.getRoot(), castToRowExpression(rewrittenAfterSubqueries)));//idAllocator.getNextId(), subPlan.getRoot(), castToRowExpression(rewrittenAfterSubqueries)));
    }

    private PlanBuilder project(PlanBuilder subPlan, Iterable<Expression> expressions, RelationPlan parentRelationPlan)
    {
        return project(subPlan, Iterables.concat(expressions, toSymbolReferences(parentRelationPlan.getFieldMappings())));
    }

    private PlanBuilder project(PlanBuilder subPlan, Iterable<Expression> expressions)
    {
        TranslationMap outputTranslations = new TranslationMap(subPlan.getRelationPlan(), analysis);

        Assignments.Builder projections = Assignments.builder();
        for (Expression expression : expressions) {
            if (expression instanceof SymbolReference) {
                ColumnReferenceExpression variable = variableAllocator.toVariableReference(expression);
                projections.put(variable, castToRowExpression(expression));
                outputTranslations.put(expression, variable);
                continue;
            }

            ColumnReferenceExpression variable = variableAllocator.newVariable(expression, analysis.getTypeWithCoercions(expression));
            projections.put(variable, castToRowExpression(subPlan.rewrite(expression)));
            outputTranslations.put(expression, variable);
        }

        return new PlanBuilder(outputTranslations, new ProjectOperator((RowNode)subPlan.getRoot(), projections.build().getMap()));
//                idAllocator.getNextId(),
//                subPlan.getRoot(),
//                projections.build()));
    }
//
//    private Map<ColumnReferenceExpression, RowExpression> coerce(Iterable<? extends Expression> expressions, PlanBuilder subPlan, TranslationMap translations)
//    {
//        ImmutableMap.Builder<ColumnReferenceExpression, RowExpression> projections = ImmutableMap.builder();
//
//        for (Expression expression : expressions) {
//            Type type = analysis.getType(expression).get();
//            Type coercion = analysis.getCoercion(expression);
//            ColumnReferenceExpression variable = variableAllocator.newVariable(expression, firstNonNull(coercion, type));
//            Expression rewritten = subPlan.rewrite(expression);
//            if (coercion != null) {
//                rewritten = new Cast(
//                        rewritten,
//                        coercion.getTypeSignature().toString(),
//                        false,
//                        metadata.getFunctionAndTypeManager().isTypeOnlyCoercion(type, coercion));
//            }
//            projections.put(variable, castToRowExpression(rewritten));
//            translations.put(expression, variable);
//        }
//
//        return projections.build();
//    }
//
//    private PlanBuilder explicitCoercionFields(PlanBuilder subPlan, Iterable<Expression> alreadyCoerced, Iterable<? extends Expression> uncoerced)
//    {
//        TranslationMap translations = new TranslationMap(subPlan.getRelationPlan(), analysis);
//        Assignments.Builder projections = Assignments.builder();
//
//        projections.putAll(coerce(uncoerced, subPlan, translations));
//
//        for (Expression expression : alreadyCoerced) {
//            if (expression instanceof SymbolReference) {
//                // If this is an identity projection, no need to rewrite it
//                // This is needed because certain synthetic identity expressions such as "group id" introduced when planning GROUPING
//                // don't have a corresponding analysis, so the code below doesn't work for them
//                projections.put(variableAllocator.toVariableReference(expression), castToRowExpression(expression));
//                continue;
//            }
//
//            ColumnReferenceExpression variable = variableAllocator.newVariable(expression, analysis.getType(expression));
//            Expression rewritten = subPlan.rewrite(expression);
//            projections.put(variable, castToRowExpression(rewritten));
//            translations.put(expression, variable);
//        }
//
//        return new PlanBuilder(translations, new ProjectNode(
//                idAllocator.getNextId(),
//                subPlan.getRoot(),
//                projections.build(),
//                LOCAL));
//    }
//
//    private PlanBuilder explicitCoercionVariables(PlanBuilder subPlan, List<ColumnReferenceExpression> alreadyCoerced, Iterable<? extends Expression> uncoerced)
//    {
//        TranslationMap translations = subPlan.copyTranslations();
//
//        Assignments assignments = Assignments.builder()
//                .putAll(coerce(uncoerced, subPlan, translations))
//                .putAll(identitiesAsSymbolReferences(alreadyCoerced))
//                .build();
//
//        return new PlanBuilder(translations, new ProjectNode(
//                idAllocator.getNextId(),
//                subPlan.getRoot(),
//                assignments,
//                LOCAL));
//    }

    private PlanBuilder aggregate(PlanBuilder subPlan, QuerySpecification node)
    {
        if (!analysis.isAggregation(node)) {
            return subPlan;
        }

        // 1. Pre-project all scalar inputs (arguments and non-trivial group by expressions)
        Set<Expression> groupByExpressions = ImmutableSet.copyOf(analysis.getGroupByExpressions(node));

        ImmutableList.Builder<Expression> arguments = ImmutableList.builder();
        analysis.getAggregates(node).stream()
                .map(FunctionCall::getArguments)
                .flatMap(List::stream)
//                .filter(exp -> !(exp instanceof LambdaExpression)) // lambda expression is generated at execution time
                .forEach(arguments::add);
//
//        analysis.getAggregates(node).stream()
//                .map(FunctionCall::getOrderBy)
//                .filter(Optional::isPresent)
//                .map(Optional::get)
//                .map(OrderBy::getSortItems)
//                .flatMap(List::stream)
//                .map(SortItem::getSortKey)
//                .forEach(arguments::add);

        Iterable<Expression> inputs = Iterables.concat(groupByExpressions, arguments.build());
//        subPlan = handleSubqueries(subPlan, node, inputs);

        if (!Iterables.isEmpty(inputs)) { // avoid an empty projection if the only aggregation is COUNT (which has no arguments)
            subPlan = project(subPlan, inputs);
        }

        // 2. Aggregate

        // 2.a. Rewrite aggregate arguments
        TranslationMap argumentTranslations = new TranslationMap(subPlan.getRelationPlan(), analysis);

        ImmutableList.Builder<ColumnReferenceExpression> aggregationArgumentsBuilder = ImmutableList.builder();
        for (Expression argument : arguments.build()) {
            ColumnReferenceExpression variable = subPlan.translate(argument);
            argumentTranslations.put(argument, variable);
            aggregationArgumentsBuilder.add(variable);
        }
        List<ColumnReferenceExpression> aggregationArguments = aggregationArgumentsBuilder.build();

        // 2.b. Rewrite grouping columns
        TranslationMap groupingTranslations = new TranslationMap(subPlan.getRelationPlan(), analysis);
        Map<ColumnReferenceExpression, ColumnReferenceExpression> groupingSetMappings = new LinkedHashMap<>();

        for (Expression expression : groupByExpressions) {
            ColumnReferenceExpression input = subPlan.translate(expression);
            ColumnReferenceExpression output = variableAllocator.newVariable(expression, analysis.getTypeWithCoercions(expression), "gid");
            groupingTranslations.put(expression, output);
            groupingSetMappings.put(output, input);
        }

        // This tracks the grouping sets before complex expressions are considered (see comments below)
        // It's also used to compute the descriptors needed to implement grouping()
        List<Set<FieldId>> columnOnlyGroupingSets = ImmutableList.of(ImmutableSet.of());
        List<List<ColumnReferenceExpression>> groupingSets = ImmutableList.of();

        if (node.getGroupBy().isPresent()) {
            // For the purpose of "distinct", we need to canonicalize column references that may have varying
            // syntactic forms (e.g., "t.a" vs "a"). Thus we need to enumerate grouping sets based on the underlying
            // fieldId associated with each column reference expression.

            // The catch is that simple group-by expressions can be arbitrary expressions (this is a departure from the SQL specification).
            // But, they don't affect the number of grouping sets or the behavior of "distinct" . We can compute all the candidate
            // grouping sets in terms of fieldId, dedup as appropriate and then cross-join them with the complex expressions.
            List<Set<FieldId>> gs = analysis.getGroupingSet(node);//todo: should be a list or a set?
            Preconditions.checkNotNull(gs, "Grouping set should not be null {}", node);
//            Analysis.GroupingSetAnalysis groupingSetAnalysis = analysis.getGroupingSets(node);
//            columnOnlyGroupingSets = enumerateGroupingSets(groupingSetAnalysis);
//
//            if (node.getGroupBy().get().isDistinct()) {
//                columnOnlyGroupingSets = columnOnlyGroupingSets.stream()
//                        .distinct()
//                        .collect(toImmutableList());
//            }

            // add in the complex expressions an turn materialize the grouping sets in terms of plan columns
            ImmutableList.Builder<List<ColumnReferenceExpression>> groupingSetBuilder = ImmutableList.builder();
            for (Set<FieldId> groupingSet : columnOnlyGroupingSets) {
                ImmutableList.Builder<ColumnReferenceExpression> columns = ImmutableList.builder();
//                groupingSetAnalysis.getComplexExpressions().stream()
//                    .map(groupingTranslations::get)
//                    .forEach(columns::add);

                groupingSet.stream()
                    .map(field -> groupingTranslations.get(new FieldReference(field.getFieldIndex())))
                    .forEach(columns::add);

                groupingSetBuilder.add(columns.build());
            }

            groupingSets = groupingSetBuilder.build();
        }

        // 2.c. Generate GroupIdNode (multiple grouping sets) or ProjectNode (single grouping set)
        Optional<ColumnReferenceExpression> groupIdVariable = Optional.empty();
//        if (groupingSets.size() > 1) {
//            groupIdVariable = Optional.of(variableAllocator.newVariable("groupId", BIGINT));
//            GroupIdNode groupId = new GroupIdNode(idAllocator.getNextId(), subPlan.getRoot(), groupingSets, groupingSetMappings, aggregationArguments, groupIdVariable.get());
//            subPlan = new PlanBuilder(groupingTranslations, groupId);
//        }
//        else {
            Assignments.Builder assignments = Assignments.builder();
            aggregationArguments.stream().map(AssignmentUtils::identityAsSymbolReference).forEach(assignments::put);
            groupingSetMappings.forEach((key, value) -> assignments.put(key, castToRowExpression(asSymbolReference(value))));

            ProjectOperator project = new ProjectOperator((RowNode)subPlan.getRoot(), assignments.build().getMap());//idAllocator.getNextId(), subPlan.getRoot(), assignments.build());
            subPlan = new PlanBuilder(groupingTranslations, project);
//        }

        TranslationMap aggregationTranslations = new TranslationMap(subPlan.getRelationPlan(), analysis);
        aggregationTranslations.copyMappingsFrom(groupingTranslations);

        // 2.d. Rewrite aggregates
        ImmutableMap.Builder<ColumnReferenceExpression, Aggregation> aggregationsBuilder = ImmutableMap.builder();
        boolean needPostProjectionCoercion = false;
        for (FunctionCall aggregate : analysis.getAggregates(node)) {
            Expression rewritten = argumentTranslations.rewrite(aggregate);
            ColumnReferenceExpression newVariable = variableAllocator.newVariable(rewritten, analysis.getType(aggregate));

            // TODO: this is a hack, because we apply coercions to the output of expressions, rather than the arguments to expressions.
            // Therefore we can end up with this implicit cast, and have to move it into a post-projection
            if (rewritten instanceof Cast) {
                rewritten = ((Cast) rewritten).getExpression();
                needPostProjectionCoercion = true;
            }
            aggregationTranslations.put(aggregate, newVariable);
            FunctionCall rewrittenFunction = (FunctionCall) rewritten;

            aggregationsBuilder.put(newVariable,
                    new Aggregation(null, null));
//                            new CallExpression(
//                                    aggregate.getName().getSuffix().getCanonical(),
//                                    analysis.getFunctionHandle(aggregate),
//                                    analysis.getType(aggregate).get(),
//                                    rewrittenFunction.getArguments().stream().map(OriginalExpressionUtils::castToRowExpression).collect(toImmutableList()))
////                            rewrittenFunction.getFilter().map(OriginalExpressionUtils::castToRowExpression),
////                            rewrittenFunction.getOrderBy().map(orderBy -> toOrderingScheme(orderBy, variableAllocator.getTypes())),
////                            rewrittenFunction.isDistinct(),
//                            ));
        }
        Map<ColumnReferenceExpression, Aggregation> aggregations = aggregationsBuilder.build();

//        ImmutableSet.Builder<Integer> globalGroupingSets = ImmutableSet.builder();
//        for (int i = 0; i < groupingSets.size(); i++) {
//            if (groupingSets.get(i).isEmpty()) {
//                globalGroupingSets.add(i);
//            }
//        }

        ImmutableList.Builder<ColumnReferenceExpression> groupingKeys = ImmutableList.builder();
//        groupingSets.stream()
//                .flatMap(List::stream)
//                .distinct()
//                .forEach(groupingKeys::add);
        groupIdVariable.ifPresent(groupingKeys::add);

        AggregateOperator aggregationNode = new AggregateOperator((RowNode) subPlan.getRoot(),
            new LinkedHashMap<>(),
            new LinkedHashMap<>()
        );
//                idAllocator.getNextId(),
//                subPlan.getRoot(),
//                aggregations,
//                groupingSets,
//                groupingSets(
//                        groupingKeys.build(),
//                        groupingSets.size(),
//                        globalGroupingSets.build()),
//                ImmutableList.of(),
//                AggregationNode.Step.SINGLE,
//                Optional.empty(),
//                groupIdVariable);

        subPlan = new PlanBuilder(aggregationTranslations, aggregationNode);

        // 3. Post-projection
        // Add back the implicit casts that we removed in 2.a
        // TODO: this is a hack, we should change type coercions to coerce the inputs to functions/operators instead of coercing the output
//        if (needPostProjectionCoercion) {
//            ImmutableList.Builder<Expression> alreadyCoerced = ImmutableList.builder();
//            alreadyCoerced.addAll(groupByExpressions);
//            groupIdVariable.map(variable -> new SymbolReference(variable.getName())).ifPresent(alreadyCoerced::add);

//            subPlan = explicitCoercionFields(subPlan, alreadyCoerced.build(), analysis.getAggregates(node));
//        }

        // 4. Project and re-write all grouping functions
        return handleGroupingOperations(subPlan, node, groupIdVariable, columnOnlyGroupingSets);
    }
//
//    private List<Set<FieldId>> enumerateGroupingSets(Analysis.GroupingSetAnalysis groupingSetAnalysis)
//    {
//        List<List<Set<FieldId>>> partialSets = new ArrayList<>();
//
//        for (Set<FieldId> cube : groupingSetAnalysis.getCubes()) {
//            partialSets.add(ImmutableList.copyOf(Sets.powerSet(cube)));
//        }
//
//        for (List<FieldId> rollup : groupingSetAnalysis.getRollups()) {
//            List<Set<FieldId>> sets = IntStream.rangeClosed(0, rollup.size())
//                    .mapToObj(i -> ImmutableSet.copyOf(rollup.subList(0, i)))
//                    .collect(toImmutableList());
//
//            partialSets.add(sets);
//        }
//
//        partialSets.addAll(groupingSetAnalysis.getOrdinarySets());
//
//        if (partialSets.isEmpty()) {
//            return ImmutableList.of(ImmutableSet.of());
//        }
//
//        // compute the cross product of the partial sets
//        List<Set<FieldId>> allSets = new ArrayList<>();
//        partialSets.get(0)
//                .stream()
//                .map(ImmutableSet::copyOf)
//                .forEach(allSets::add);
//
//        for (int i = 1; i < partialSets.size(); i++) {
//            List<Set<FieldId>> groupingSets = partialSets.get(i);
//            List<Set<FieldId>> oldGroupingSetsCrossProduct = ImmutableList.copyOf(allSets);
//            allSets.clear();
//            for (Set<FieldId> existingSet : oldGroupingSetsCrossProduct) {
//                for (Set<FieldId> groupingSet : groupingSets) {
//                    Set<FieldId> concatenatedSet = ImmutableSet.<FieldId>builder()
//                            .addAll(existingSet)
//                            .addAll(groupingSet)
//                            .build();
//                    allSets.add(concatenatedSet);
//                }
//            }
//        }
//
//        return allSets;
//    }

    private PlanBuilder handleGroupingOperations(PlanBuilder subPlan, QuerySpecification node, Optional<ColumnReferenceExpression> groupIdVariable, List<Set<FieldId>> groupingSets)
    {
        return subPlan; //todo?
//        if (analysis.getGroupingOperations(node).isEmpty()) {
//            return subPlan;
//        }
//
//        TranslationMap newTranslations = subPlan.copyTranslations();
//
//        Assignments.Builder projections = Assignments.builder();
//        projections.putAll(identitiesAsSymbolReferences(subPlan.getRoot().getOutputVariables()));
//
//        List<Set<Integer>> descriptor = groupingSets.stream()
//                .map(set -> set.stream()
//                        .map(FieldId::getFieldIndex)
//                        .collect(toImmutableSet()))
//                .collect(toImmutableList());
//
//        for (GroupingOperation groupingOperation : analysis.getGroupingOperations(node)) {
//            Expression rewritten = GroupingOperationRewriter.rewriteGroupingOperation(groupingOperation, descriptor, analysis.getColumnReferenceFields(), groupIdVariable);
//            Type coercion = analysis.getCoercion(groupingOperation);
//            ColumnReferenceExpression variable = variableAllocator.newVariable(rewritten, analysis.getTypeWithCoercions(groupingOperation));
//            if (coercion != null) {
//                rewritten = new Cast(
//                        rewritten,
//                        coercion.getTypeSignature().toString(),
//                        false,
//                        metadata.getFunctionAndTypeManager().isTypeOnlyCoercion(analysis.getType(groupingOperation), coercion));
//            }
//            projections.put(variable, castToRowExpression(rewritten));
//            newTranslations.put(groupingOperation, variable);
//        }
//
//        return new PlanBuilder(newTranslations, new ProjectNode(idAllocator.getNextId(), subPlan.getRoot(), projections.build(), LOCAL));
    }
//
//    private PlanBuilder window(PlanBuilder subPlan, OrderBy node)
//    {
//        return window(subPlan, ImmutableList.copyOf(analysis.getOrderByWindowFunctions(node)));
//    }
//
//    private PlanBuilder window(PlanBuilder subPlan, QuerySpecification node)
//    {
//        return window(subPlan, ImmutableList.copyOf(analysis.getWindowFunctions(node)));
//    }
//
//    private PlanBuilder window(PlanBuilder subPlan, List<FunctionCall> windowFunctions)
//    {
//        if (windowFunctions.isEmpty()) {
//            return subPlan;
//        }
//
//        for (FunctionCall windowFunction : windowFunctions) {
//            Window window = windowFunction.getWindow().get();
//
//            // Extract frame
//            WindowFrame.Type frameType = WindowFrame.Type.RANGE;
//            FrameBound.Type frameStartType = FrameBound.Type.UNBOUNDED_PRECEDING;
//            FrameBound.Type frameEndType = FrameBound.Type.CURRENT_ROW;
//            Expression frameStart = null;
//            Expression frameEnd = null;
//
//            if (window.getFrame().isPresent()) {
//                WindowFrame frame = window.getFrame().get();
//                frameType = frame.getType();
//
//                frameStartType = frame.getStart().getType();
//                frameStart = frame.getStart().getValue().orElse(null);
//
//                if (frame.getEnd().isPresent()) {
//                    frameEndType = frame.getEnd().get().getType();
//                    frameEnd = frame.getEnd().get().getValue().orElse(null);
//                }
//            }
//
//            // Pre-project inputs
//            ImmutableList.Builder<Expression> inputs = ImmutableList.<Expression>builder()
//                    .addAll(windowFunction.getArguments())
//                    .addAll(window.getPartitionBy())
//                    .addAll(Iterables.transform(getSortItemsFromOrderBy(window.getOrderBy()), SortItem::getSortKey));
//
//            if (frameStart != null) {
//                inputs.add(frameStart);
//            }
//            if (frameEnd != null) {
//                inputs.add(frameEnd);
//            }
//
//            subPlan = subPlan.appendProjections(inputs.build(), variableAllocator, idAllocator);
//
//            // Rewrite PARTITION BY in terms of pre-projected inputs
//            ImmutableList.Builder<ColumnReferenceExpression> partitionByVariables = ImmutableList.builder();
//            for (Expression expression : window.getPartitionBy()) {
//                partitionByVariables.add(subPlan.translateToVariable(expression));
//            }
//
//            // Rewrite ORDER BY in terms of pre-projected inputs
//            LinkedHashMap<ColumnReferenceExpression, SortOrder> orderings = new LinkedHashMap<>();
//            for (SortItem item : getSortItemsFromOrderBy(window.getOrderBy())) {
//                ColumnReferenceExpression variable = subPlan.translateToVariable(item.getSortKey());
//                // don't override existing keys, i.e. when "ORDER BY a ASC, a DESC" is specified
//                orderings.putIfAbsent(variable, toSortOrder(item));
//            }
//
//            // Rewrite frame bounds in terms of pre-projected inputs
//            Optional<ColumnReferenceExpression> frameStartVariable = Optional.empty();
//            Optional<ColumnReferenceExpression> frameEndVariable = Optional.empty();
//            if (frameStart != null) {
//                frameStartVariable = Optional.of(subPlan.translate(frameStart));
//            }
//            if (frameEnd != null) {
//                frameEndVariable = Optional.of(subPlan.translate(frameEnd));
//            }
//
//            WindowNode.Frame frame = new WindowNode.Frame(
//                    toWindowType(frameType),
//                    toBoundType(frameStartType),
//                    frameStartVariable,
//                    toBoundType(frameEndType),
//                    frameEndVariable,
//                    Optional.ofNullable(frameStart).map(Expression::toString),
//                    Optional.ofNullable(frameEnd).map(Expression::toString));
//
//            TranslationMap outputTranslations = subPlan.copyTranslations();
//
//            // Rewrite function call in terms of pre-projected inputs
//            Expression rewritten = subPlan.rewrite(windowFunction);
//
//            boolean needCoercion = rewritten instanceof Cast;
//            // Strip out the cast and add it back as a post-projection
//            if (rewritten instanceof Cast) {
//                rewritten = ((Cast) rewritten).getExpression();
//            }
//
//            // If refers to existing variable, don't create another PlanNode
//            if (rewritten instanceof SymbolReference) {
//                if (needCoercion) {
//                    subPlan = explicitCoercionVariables(subPlan, subPlan.getRoot().getOutputVariables(), ImmutableList.of(windowFunction));
//                }
//
//                continue;
//            }
//
//            Type returnType = analysis.getType(windowFunction);
//            ColumnReferenceExpression newVariable = variableAllocator.newVariable(rewritten, returnType);
//            outputTranslations.put(windowFunction, newVariable);
//
//            // TODO: replace arguments with RowExpression once we introduce subquery expression for RowExpression (#12745).
//            // Wrap all arguments in CallExpression to be RawExpression.
//            // The utility that work on the CallExpression should be aware of the RawExpression handling.
//            // The interface will be dirty until we introduce subquery expression for RowExpression.
//            // With subqueries, the translation from Expression to RowExpression can happen here.
//            WindowNode.Function function = new WindowNode.Function(
//                    call(
//                            windowFunction.getName().toString(),
//                            analysis.getFunctionHandle(windowFunction),
//                            returnType,
//                            ((FunctionCall) rewritten).getArguments().stream().map(OriginalExpressionUtils::castToRowExpression).collect(toImmutableList())),
//                    frame,
//                    windowFunction.isIgnoreNulls());
//
//            ImmutableList.Builder<ColumnReferenceExpression> orderByVariables = ImmutableList.builder();
//            orderByVariables.addAll(orderings.keySet());
//            Optional<OrderingScheme> orderingScheme = Optional.empty();
//            if (!orderings.isEmpty()) {
//                orderingScheme = Optional.of(new OrderingScheme(orderByVariables.build().stream().map(variable -> new Ordering(variable, orderings.get(variable))).collect(toImmutableList())));
//            }
//
//            // create window node
//            subPlan = new PlanBuilder(outputTranslations,
//                    new WindowNode(
//                            idAllocator.getNextId(),
//                            subPlan.getRoot(),
//                            new WindowNode.Specification(
//                                    partitionByVariables.build(),
//                                    orderingScheme),
//                            ImmutableMap.of(newVariable, function),
//                            Optional.empty(),
//                            ImmutableSet.of(),
//                            0));
//
//            if (needCoercion) {
//                subPlan = explicitCoercionVariables(subPlan, subPlan.getRoot().getOutputVariables(), ImmutableList.of(windowFunction));
//            }
//        }
//
//        return subPlan;
//    }
//
//    private PlanBuilder handleSubqueries(PlanBuilder subPlan, Node node, Iterable<Expression> inputs)
//    {
//        for (Expression input : inputs) {
//            subPlan = subqueryPlanner.handleSubqueries(subPlan, subPlan.rewrite(input), node);
//        }
//        return subPlan;
//    }
//
//    private PlanBuilder distinct(PlanBuilder subPlan, QuerySpecification node)
//    {
//        if (node.getSelect().isDistinct()) {
//            return subPlan.withNewRoot(
//                    new AggregationNode(
//                            idAllocator.getNextId(),
//                            subPlan.getRoot(),
//                            ImmutableMap.of(),
//                            singleGroupingSet(subPlan.getRoot().getOutputVariables()),
//                            ImmutableList.of(),
//                            AggregationNode.Step.SINGLE,
//                            Optional.empty(),
//                            Optional.empty()));
//        }
//
//        return subPlan;
//    }
//
//    private PlanBuilder sort(PlanBuilder subPlan, Query node)
//    {
//        return sort(subPlan, node.getOrderBy(), analysis.getOrderByExpressions(node));
//    }
//
//    private PlanBuilder sort(PlanBuilder subPlan, QuerySpecification node)
//    {
//        return sort(subPlan, node.getOrderBy(), analysis.getOrderByExpressions(node));
//    }
//
//    private PlanBuilder sort(PlanBuilder subPlan, Optional<OrderBy> orderBy, List<Expression> orderByExpressions)
//    {
//        if (!orderBy.isPresent() || (isSkipRedundantSort(session)) && analysis.isOrderByRedundant(orderBy.get())) {
//            return subPlan;
//        }
//
//        PlanNode planNode;
//        OrderingScheme orderingScheme = toOrderingScheme(
//                orderByExpressions.stream().map(subPlan::translate).collect(toImmutableList()),
//                orderBy.get().getSortItems().stream().map(PlannerUtils::toSortOrder).collect(toImmutableList()));
//        planNode = new SortNode(idAllocator.getNextId(), subPlan.getRoot(), orderingScheme, false);
//
//        return subPlan.withNewRoot(planNode);
//    }
//
//    private PlanBuilder limit(PlanBuilder subPlan, Query node)
//    {
//        return limit(subPlan, node.getLimit());
//    }
//
//    private PlanBuilder limit(PlanBuilder subPlan, QuerySpecification node)
//    {
//        return limit(subPlan, node.getLimit());
//    }
//
//    private PlanBuilder limit(PlanBuilder subPlan, Optional<String> limit)
//    {
//        if (!limit.isPresent()) {
//            return subPlan;
//        }
//
//        if (!limit.get().equalsIgnoreCase("all")) {
//            long limitValue = Long.parseLong(limit.get());
//            subPlan = subPlan.withNewRoot(new LimitNode(idAllocator.getNextId(), subPlan.getRoot(), limitValue, FINAL));
//        }
//
//        return subPlan;
//    }

    private static List<Expression> toSymbolReferences(List<ColumnReferenceExpression> variables)
    {
        return variables.stream()
                .map(variable -> new SymbolReference(variable.getName()))
                .collect(toImmutableList());
    }
}
