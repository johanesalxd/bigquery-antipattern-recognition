package com.google.zetasql.toolkit.antipattern.analyzer; // Same package? OK.

import com.google.zetasql.toolkit.catalog.bigquery.BigQueryCatalog;
import com.google.zetasql.toolkit.catalog.bigquery.BigQueryAPIResourceProvider;
import com.google.zetasql.toolkit.catalog.bigquery.BigQueryService;
import com.google.zetasql.toolkit.ZetaSQLToolkitAnalyzer;
import com.google.zetasql.toolkit.antipattern.analyzer.visitors.clustering.clusteringkeyused.ClusteringKeysUsedVisitor;
import com.google.zetasql.AnalyzerOptions;
import com.google.zetasql.toolkit.options.BigQueryLanguageOptions;
import com.google.zetasql.resolvedast.ResolvedNodes;
import com.google.zetasql.LanguageOptions;

import java.util.Arrays; // For List creation
import java.util.HashMap; // For Map creation
import java.util.Iterator;
import java.util.List; // For List creation
import java.util.Map; // For Map creation
import static org.junit.Assert.assertEquals;
import org.junit.Before;
import org.junit.Test;


public class IdentifyUnusedClusteringKeysTest {

    LanguageOptions languageOptions;
    AnalyzerOptions analyzerOptions;
    ZetaSQLToolkitAnalyzer zetaSQLToolkitAnalyzer;
    BigQueryService service; // May not be needed directly if service calls are mocked/avoided
    BigQueryAPIResourceProvider resourceProvider;
    BigQueryCatalog catalog;
    String PUBLIC_TESTING_PROJECT = "bigquery-public-data";
    String PUBLIC_CLUSTERED_TABLE = "`bigquery-public-data.wikipedia.pageviews_2025`";
    List<String> PUBLIC_CLUSTERING_COLUMNS = Arrays.asList("wiki", "title");

    @Before
    public void setUp() {
      languageOptions = new LanguageOptions();
      languageOptions = BigQueryLanguageOptions.get().enableMaximumLanguageFeatures();
      languageOptions.setSupportsAllStatementKinds();
      analyzerOptions = new AnalyzerOptions();
      analyzerOptions.setLanguageOptions(languageOptions);
      zetaSQLToolkitAnalyzer = new ZetaSQLToolkitAnalyzer(analyzerOptions);
      service = BigQueryService.buildDefault();
      resourceProvider = BigQueryAPIResourceProvider.build(service);

      catalog = new BigQueryCatalog(PUBLIC_TESTING_PROJECT, resourceProvider);
      catalog.addAllTablesUsedInQuery("SELECT wiki FROM " + PUBLIC_CLUSTERED_TABLE + " LIMIT 0", analyzerOptions);
    }

    private Map<String, List<String>> getClusteringInfoForTable() {
        Map<String, List<String>> clusteringInfo = new HashMap<>();
        clusteringInfo.put(PUBLIC_CLUSTERED_TABLE, PUBLIC_CLUSTERING_COLUMNS);
        return clusteringInfo;
    }

    @Test
    public void clusteringKeysUsedInWhere() {
      String expected = "";
      String query = "SELECT wiki "
          + "FROM " + PUBLIC_CLUSTERED_TABLE + "\n"
          + "WHERE wiki = 'sf'";

      Iterator<ResolvedNodes.ResolvedStatement> statementIterator = zetaSQLToolkitAnalyzer.analyzeStatements(query, catalog);

      Map<String, List<String>> clusteringInfo = getClusteringInfoForTable();
      ClusteringKeysUsedVisitor visitor = new ClusteringKeysUsedVisitor(clusteringInfo);
      statementIterator.forEachRemaining(statement -> statement.accept(visitor));
      visitor.analyzeKeyUsage(); // Analyze after visiting
      String recommendation = visitor.getResult();

      assertEquals(expected, recommendation);
    }

    @Test
    public void clusteringKeysNotUsedInWhere() {
      String expected = "Table: `bigquery-public-data.wikipedia.pageviews_2025` is clustered by [wiki, title], but these keys were not referenced in WHERE, JOIN ON, or GROUP BY clauses. Clustering might not provide significant benefits for this query.";
      String query = "SELECT wiki "
          + "FROM " + PUBLIC_CLUSTERED_TABLE + "\n"
          + "WHERE views = 1";

      Iterator<ResolvedNodes.ResolvedStatement> statementIterator = zetaSQLToolkitAnalyzer.analyzeStatements(query, catalog);

      Map<String, List<String>> clusteringInfo = getClusteringInfoForTable();
      ClusteringKeysUsedVisitor visitor = new ClusteringKeysUsedVisitor(clusteringInfo);
      statementIterator.forEachRemaining(statement -> statement.accept(visitor));
      visitor.analyzeKeyUsage(); // Analyze after visiting
      String recommendation = visitor.getResult();
      System.out.println(recommendation);

      assertEquals(expected, recommendation);
    }

    // @Test
    // public void clusteringKeysUsedInGroupBy() {
    //    String expected = ""; // Expect no warning
    //    String query = "SELECT committer.name, count(*) "
    //        + "FROM " + CLUSTERED_TABLE + "\n"
    //        + "GROUP BY committer.name"; // Uses a clustering key in GROUP BY (committer.name is tricky, it's nested)
    //                                      // Let's use repo_name for simplicity unless confident parser resolves nested fields correctly for check
    //    query = "SELECT repo_name, count(*) "
    //        + "FROM " + CLUSTERED_TABLE + "\n"
    //        + "GROUP BY repo_name"; // Uses a clustering key in GROUP BY

    //    Iterator<ResolvedNodes.ResolvedStatement> statementIterator = zetaSQLToolkitAnalyzer.analyzeStatements(query, catalog);
    //    Map<String, List<String>> clusteringInfo = getClusteringInfoForCommitsTable();
    //    ClusteringKeysUsedVisitor visitor = new ClusteringKeysUsedVisitor(clusteringInfo);
    //    statementIterator.forEachRemaining(statement -> statement.accept(visitor));
    //    visitor.analyzeKeyUsage();
    //    String recommendation = visitor.getResult();

    //    assertEquals(expected, recommendation);
    // }

    //  @Test
    //  public void clusteringKeysUsedInJoin() {
    //     // NOTE: This test requires the schema for the joined table in the catalog.
    //     // For simplicity, we'll skip the full join test unless a mock catalog setup is available.
    //     // We'll test the WHERE clause usage which covers predicate usage.
    //     // If testing JOIN:
    //     // 1. Define schema for a dummy table like `other_repos(repo_name STRING)`
    //     // 2. Add dummy table to catalog: catalog.addTable("other_repos", List.of(SimpleColumn("repo_name", typeFactory.createStringType())));
    //     // 3. Write JOIN query: SELECT c.commit FROM CLUSTERED_TABLE c JOIN other_repos o ON c.repo_name = o.repo_name
    //     // 4. Run test as others... expected = ""
    //      String expected = ""; // Using WHERE test instead for simplicity
    //      String query = "SELECT commit "
    //          + "FROM " + CLUSTERED_TABLE + "\n"
    //          + "WHERE repo_name = 'some/repo'"; // Uses a clustering key in WHERE

    //      Iterator<ResolvedNodes.ResolvedStatement> statementIterator = zetaSQLToolkitAnalyzer.analyzeStatements(query, catalog);
    //      Map<String, List<String>> clusteringInfo = getClusteringInfoForCommitsTable();
    //      ClusteringKeysUsedVisitor visitor = new ClusteringKeysUsedVisitor(clusteringInfo);
    //      statementIterator.forEachRemaining(statement -> statement.accept(visitor));
    //      visitor.analyzeKeyUsage();
    //      String recommendation = visitor.getResult();
    //      assertEquals(expected, recommendation); // Re-asserting based on WHERE usage
    //  }


    // @Test
    // public void clusteringKeysNotUsed() {
    //   String expected = String.format(
    //       "Table: %s is clustered by [repo_name, committer.name], but these keys were not referenced in WHERE, JOIN ON, or GROUP BY clauses. Clustering might not provide significant benefits for this query.",
    //        CLUSTERED_TABLE_NAME_FOR_MAP);
    //   String query = "SELECT subject "
    //       + "FROM " + CLUSTERED_TABLE + "\n"
    //       + "WHERE commit = 0x123abc"; // Filter on a non-clustering key

    //   Iterator<ResolvedNodes.ResolvedStatement> statementIterator = zetaSQLToolkitAnalyzer.analyzeStatements(query, catalog);
    //   Map<String, List<String>> clusteringInfo = getClusteringInfoForCommitsTable();
    //   ClusteringKeysUsedVisitor visitor = new ClusteringKeysUsedVisitor(clusteringInfo);
    //   statementIterator.forEachRemaining(statement -> statement.accept(visitor));
    //   visitor.analyzeKeyUsage();
    //   String recommendation = visitor.getResult();

    //   assertEquals(expected, recommendation);
    // }

    // @Test
    // public void onlyOneClusteringKeyUsed() {
    //   String expected = ""; // Expect no warning as at least one key is used
    //   String query = "SELECT subject "
    //       + "FROM " + CLUSTERED_TABLE + "\n"
    //       + "WHERE repo_name = 'another/repo'"; // Filter on only one of the clustering keys

    //   Iterator<ResolvedNodes.ResolvedStatement> statementIterator = zetaSQLToolkitAnalyzer.analyzeStatements(query, catalog);
    //   Map<String, List<String>> clusteringInfo = getClusteringInfoForCommitsTable();
    //   ClusteringKeysUsedVisitor visitor = new ClusteringKeysUsedVisitor(clusteringInfo);
    //   statementIterator.forEachRemaining(statement -> statement.accept(visitor));
    //   visitor.analyzeKeyUsage();
    //   String recommendation = visitor.getResult();

    //   assertEquals(expected, recommendation);
    // }

    //  @Test
    //  public void queryWithNoClusteredTables() {
    //      // Test case where the input map is empty
    //      String expected = "";
    //      String query = "SELECT * FROM `bigquery-public-data.samples.shakespeare`"; // A non-clustered table

    //      catalog.addAllTablesUsedInQuery(query, analyzerOptions); // Add this table too
    //      Iterator<ResolvedNodes.ResolvedStatement> statementIterator = zetaSQLToolkitAnalyzer.analyzeStatements(query, catalog);

    //      Map<String, List<String>> clusteringInfo = new HashMap<>(); // Empty map

    //      ClusteringKeysUsedVisitor visitor = new ClusteringKeysUsedVisitor(clusteringInfo);
    //      statementIterator.forEachRemaining(statement -> statement.accept(visitor));
    //      visitor.analyzeKeyUsage();
    //      String recommendation = visitor.getResult();

    //      assertEquals(expected, recommendation);
    //  }

}
