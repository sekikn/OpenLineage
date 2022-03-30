package io.openlineage.spark3.agent.lifecycle.plan.columnLineage;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.api.OpenLineageContext;
import java.util.Arrays;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.types.StructType;

/**
 * Utility functions for detecting column level lineage within {@link
 * org.apache.spark.sql.catalyst.plans.logical.LogicalPlan}.
 */
@Slf4j
public class ColumnLevelLineageUtils {

  public static OpenLineage.ColumnLineageDatasetFacet buildColumnLineageDatasetFacet(
      OpenLineageContext context, StructType outputSchema, LogicalPlan plan) {

    ColumnLevelLineageBuilder builder = new ColumnLevelLineageBuilder(outputSchema);

    new FieldDependenciesCollector(plan).collect(builder);
    new OutputFieldsCollector(plan).collect(builder);
    new InputFieldsCollector(plan, context).collect(builder);

    OpenLineage.ColumnLineageDatasetFacetFieldsBuilder fieldsBuilder =
        context.getOpenLineage().newColumnLineageDatasetFacetFieldsBuilder();

    log.info(builder.toString());
    log.info(plan.prettyJson());

    Arrays.stream(outputSchema.fields())
        .forEach(
            field ->
                fieldsBuilder.put(
                    field.name(),
                    builder.getInputsUsedFor(field.name()).stream()
                        .map(
                            pair ->
                                context
                                    .getOpenLineage()
                                    .newColumnLineageDatasetFacetFieldsAdditionalBuilder()
                                    .namespace(pair.getLeft().getNamespace())
                                    .name(pair.getLeft().getName())
                                    .field(pair.getRight())
                                    .build())
                        .collect(Collectors.toList())));

    OpenLineage.ColumnLineageDatasetFacetBuilder facetBuilder =
        context.getOpenLineage().newColumnLineageDatasetFacetBuilder();

    facetBuilder.fields(fieldsBuilder.build());
    return facetBuilder.build();
  }
}
