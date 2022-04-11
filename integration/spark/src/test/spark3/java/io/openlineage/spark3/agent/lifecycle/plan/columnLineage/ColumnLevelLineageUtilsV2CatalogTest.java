package io.openlineage.spark3.agent.lifecycle.plan.columnLineage;

import static org.junit.jupiter.api.Assertions.assertTrue;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.agent.client.OpenLineageClient;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark3.agent.utils.LastQueryExecutionSparkEventListener;
import java.util.Arrays;
import java.util.Optional;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.SparkSession$;
import org.apache.spark.sql.catalyst.expressions.GenericRow;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.types.IntegerType$;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StringType$;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import scala.collection.immutable.HashMap;

@Slf4j
public class ColumnLevelLineageUtilsV2CatalogTest {

  SparkSession spark;

  OpenLineageContext context;
  StructType schema =
      new StructType(
          new StructField[] {
            new StructField("a", IntegerType$.MODULE$, false, new Metadata(new HashMap<>())),
            new StructField("b", IntegerType$.MODULE$, false, new Metadata(new HashMap<>()))
          });

  @BeforeAll
  @SneakyThrows
  public static void beforeAll() {
    SparkSession$.MODULE$.cleanupAnyExistingSession();
  }

  @AfterAll
  @SneakyThrows
  public static void afterAll() {
    SparkSession$.MODULE$.cleanupAnyExistingSession();
  }

  @BeforeEach
  @SneakyThrows
  public void beforeEach() {
    spark =
        SparkSession.builder()
            .master("local[*]")
            .appName("ColumnLevelLineage")
            .config("spark.extraListeners", LastQueryExecutionSparkEventListener.class.getName())
            .config("spark.driver.host", "127.0.0.1")
            .config("spark.driver.bindAddress", "127.0.0.1")
            .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.spark_catalog.type", "hive")
            .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.local.warehouse", "/tmp/column_level_lineage/")
            .config("spark.sql.catalog.local.type", "hadoop")
            .config(
                "spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
            .config("spark.driver.extraJavaOptions", "-Dderby.system.home=/tmp/derby")
            .getOrCreate();

    context =
        OpenLineageContext.builder()
            .sparkSession(Optional.of(spark))
            .sparkContext(spark.sparkContext())
            .openLineage(new OpenLineage(OpenLineageClient.OPEN_LINEAGE_CLIENT_URI))
            .build();

    FileSystem.get(spark.sparkContext().hadoopConfiguration())
        .delete(new Path("spark-warehouse"), true);
    FileSystem.get(spark.sparkContext().hadoopConfiguration())
        .delete(new Path("/tmp/column_level_lineage/"), true);

    spark.sql("DROP TABLE IF EXISTS local.db.t1");
    spark.sql("DROP TABLE IF EXISTS local.db.t2");
    spark.sql("DROP TABLE IF EXISTS local.db.t");

    spark
        .createDataFrame(Arrays.asList(new GenericRow(new Object[] {1, 2})), schema)
        .createOrReplaceTempView("temp");
  }

  @Test
  @SneakyThrows
  public void testCreateTableAsSelectWithUnion() {
    spark.sql("CREATE TABLE local.db.t1 USING iceberg AS SELECT * FROM temp");
    spark.sql("CREATE TABLE local.db.t2 USING iceberg AS SELECT * FROM temp");
    spark.sql(
        "CREATE TABLE local.db.t USING iceberg AS (SELECT * FROM local.db.t1 UNION SELECT * FROM local.db.t2)");

    LogicalPlan plan = LastQueryExecutionSparkEventListener.getLastExecutedLogicalPlan().get();
    OpenLineage.ColumnLineageDatasetFacet facet =
        ColumnLevelLineageUtils.buildColumnLineageDatasetFacet(context, schema, plan);

    assertColumnDependsOn(facet, "a", "file", "/tmp/column_level_lineage/db.t1", "a");
    assertColumnDependsOn(facet, "a", "file", "/tmp/column_level_lineage/db.t2", "a");
    assertColumnDependsOn(facet, "b", "file", "/tmp/column_level_lineage/db.t2", "b");
    assertColumnDependsOn(facet, "b", "file", "/tmp/column_level_lineage/db.t1", "b");
  }

  @Test
  @SneakyThrows
  public void testInsertIntoTableWithAlias() {
    spark.sql("CREATE TABLE local.db.t1 USING iceberg AS SELECT * FROM temp");
    spark.sql("CREATE TABLE local.db.t2 USING iceberg AS SELECT * FROM temp");
    spark.sql("INSERT INTO local.db.t2 SELECT * FROM local.db.t1");

    LogicalPlan plan = LastQueryExecutionSparkEventListener.getLastExecutedLogicalPlan().get();
    OpenLineage.ColumnLineageDatasetFacet facet =
        ColumnLevelLineageUtils.buildColumnLineageDatasetFacet(context, schema, plan);

    assertColumnDependsOn(facet, "a", "file", "/tmp/column_level_lineage/db.t1", "a");
    assertColumnDependsOn(facet, "b", "file", "/tmp/column_level_lineage/db.t1", "b");
  }

  @Test
  public void testUnaryExpression() {
    spark.sql("CREATE TABLE local.db.t1 USING iceberg AS SELECT * FROM temp");
    spark.sql("INSERT INTO local.db.t1 VALUES (1,2),(3,4),(5,6)");
    spark.sql(
        "CREATE TABLE local.db.t2 USING iceberg AS (SELECT a, b, ceil(a) as `c`, abs(b) as `d` FROM local.db.t1)");

    StructType outputSchema =
        new StructType(
            new StructField[] {
              new StructField("c", IntegerType$.MODULE$, false, new Metadata(new HashMap<>())),
              new StructField("d", IntegerType$.MODULE$, false, new Metadata(new HashMap<>()))
            });

    LogicalPlan plan = LastQueryExecutionSparkEventListener.getLastExecutedLogicalPlan().get();
    OpenLineage.ColumnLineageDatasetFacet facet =
        ColumnLevelLineageUtils.buildColumnLineageDatasetFacet(context, outputSchema, plan);

    assertColumnDependsOn(facet, "c", "file", "/tmp/column_level_lineage/db.t1", "a");
    assertColumnDependsOn(facet, "d", "file", "/tmp/column_level_lineage/db.t1", "b");
  }

  @Test
  public void testBinaryAndComplexExpression() {
    spark.sql("CREATE TABLE local.db.t1 USING iceberg AS SELECT * FROM temp");
    spark.sql("INSERT INTO local.db.t1 VALUES (1,2),(3,4),(5,6)");
    spark.sql(
        "CREATE TABLE local.db.t2 AS SELECT CONCAT(CAST(a AS STRING), CAST(b AS STRING)) as `c`, a+b as `d` FROM local.db.t1");

    StructType outputSchema =
        new StructType(
            new StructField[] {
              new StructField("c", StringType$.MODULE$, false, new Metadata(new HashMap<>())),
              new StructField("d", StringType$.MODULE$, false, new Metadata(new HashMap<>()))
            });

    LogicalPlan plan = LastQueryExecutionSparkEventListener.getLastExecutedLogicalPlan().get();
    OpenLineage.ColumnLineageDatasetFacet facet =
        ColumnLevelLineageUtils.buildColumnLineageDatasetFacet(context, outputSchema, plan);

    assertColumnDependsOn(facet, "c", "file", "/tmp/column_level_lineage/db.t1", "a");
    assertColumnDependsOn(facet, "c", "file", "/tmp/column_level_lineage/db.t1", "b");
    assertColumnDependsOn(facet, "d", "file", "/tmp/column_level_lineage/db.t1", "a");
    assertColumnDependsOn(facet, "d", "file", "/tmp/column_level_lineage/db.t1", "b");
  }

  @Test
  public void testJoinQuery() {
    spark.sql("CREATE TABLE local.db.t1 USING iceberg AS SELECT * FROM temp");
    spark.sql("CREATE TABLE local.db.t2 USING iceberg AS SELECT * FROM temp");

    spark.sql(
        "CREATE TABLE local.db.t AS (SELECT (local.db.t1.a + local.db.t2.a) as c FROM local.db.t1 JOIN local.db.t2 ON local.db.t1.a = local.db.t2.a)");

    StructType outputSchema =
        new StructType(
            new StructField[] {
              new StructField("c", IntegerType$.MODULE$, false, new Metadata(new HashMap<>())),
            });

    LogicalPlan plan = LastQueryExecutionSparkEventListener.getLastExecutedLogicalPlan().get();
    OpenLineage.ColumnLineageDatasetFacet facet =
        ColumnLevelLineageUtils.buildColumnLineageDatasetFacet(context, outputSchema, plan);

    assertColumnDependsOn(facet, "c", "file", "/tmp/column_level_lineage/db.t1", "a");
    assertColumnDependsOn(facet, "c", "file", "/tmp/column_level_lineage/db.t2", "a");
  }

  @Test
  public void testAggregateQuery() {
    spark.sql("CREATE TABLE local.db.t1 USING iceberg AS SELECT * FROM temp");
    spark.sql("CREATE TABLE local.db.t2 AS (SELECT max(a) as a FROM local.db.t1 GROUP BY a)");

    StructType outputSchema =
        new StructType(
            new StructField[] {
              new StructField("a", IntegerType$.MODULE$, false, new Metadata(new HashMap<>())),
            });

    LogicalPlan plan = LastQueryExecutionSparkEventListener.getLastExecutedLogicalPlan().get();
    OpenLineage.ColumnLineageDatasetFacet facet =
        ColumnLevelLineageUtils.buildColumnLineageDatasetFacet(context, outputSchema, plan);

    assertColumnDependsOn(facet, "a", "file", "/tmp/column_level_lineage/db.t1", "a");
  }

  @Test
  public void testCTEQuery() {
    spark.sql("CREATE TABLE local.db.t1 USING iceberg AS SELECT * FROM temp");
    spark
        .sql(
            "WITH t2(a,b) AS (SELECT * FROM local.db.t1) SELECT a AS c, b AS d FROM t2 WHERE t2.a = 1")
        .collect();

    StructType outputSchema =
        new StructType(
            new StructField[] {
              new StructField("c", IntegerType$.MODULE$, false, new Metadata(new HashMap<>())),
              new StructField("d", IntegerType$.MODULE$, false, new Metadata(new HashMap<>()))
            });

    LogicalPlan plan = LastQueryExecutionSparkEventListener.getLastExecutedLogicalPlan().get();
    OpenLineage.ColumnLineageDatasetFacet facet =
        ColumnLevelLineageUtils.buildColumnLineageDatasetFacet(context, outputSchema, plan);

    assertColumnDependsOn(facet, "c", "file", "/tmp/column_level_lineage/db.t1", "a");
    assertColumnDependsOn(facet, "d", "file", "/tmp/column_level_lineage/db.t1", "b");
  }

  private void assertColumnDependsOn(
      OpenLineage.ColumnLineageDatasetFacet facet,
      String outputColumn,
      String expectedNamespace,
      String expectedName,
      String expectedInputField) {

    facet.getFields().getAdditionalProperties().get(outputColumn).stream()
        .forEach(e -> log.info("{}:{}:{}", e.getNamespace(), e.getName(), e.getField()));

    assertTrue(
        facet.getFields().getAdditionalProperties().get(outputColumn).stream()
            .filter(f -> f.getNamespace().equalsIgnoreCase(expectedNamespace))
            .filter(f -> f.getName().equals(expectedName))
            .filter(f -> f.getField().equalsIgnoreCase(expectedInputField))
            .findAny()
            .isPresent());
  }
}
