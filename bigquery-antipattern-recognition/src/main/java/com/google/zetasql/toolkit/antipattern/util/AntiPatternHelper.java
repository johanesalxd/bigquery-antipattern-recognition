/*
 * Copyright (C) 2024 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.zetasql.toolkit.antipattern.util;

import com.google.zetasql.AnalyzerOptions;
import com.google.zetasql.LanguageOptions;
import com.google.zetasql.Parser;
import com.google.zetasql.parser.ASTNodes;
import com.google.zetasql.parser.ParseTreeVisitor;
import com.google.zetasql.resolvedast.ResolvedNodes;
import com.google.zetasql.toolkit.ZetaSQLToolkitAnalyzer;
import com.google.zetasql.toolkit.antipattern.AntiPatternVisitor;
import com.google.zetasql.toolkit.antipattern.analyzer.visitors.ClusteringCheckVisitor;
import com.google.zetasql.toolkit.antipattern.analyzer.visitors.ClusteringKeysUsedVisitor;
import com.google.zetasql.toolkit.antipattern.analyzer.visitors.PartitionCheckVisitor;
import com.google.zetasql.toolkit.antipattern.analyzer.visitors.joinorder.JoinOrderVisitor;
import com.google.zetasql.toolkit.antipattern.cmd.InputQuery;
import com.google.zetasql.toolkit.antipattern.parser.visitors.*;
import com.google.zetasql.toolkit.antipattern.parser.visitors.rownum.IdentifyLatestRecordVisitor;
import com.google.zetasql.toolkit.antipattern.parser.visitors.whereorder.IdentifyWhereOrderVisitor;
import com.google.zetasql.toolkit.catalog.bigquery.BigQueryAPIResourceProvider;
import com.google.zetasql.toolkit.catalog.bigquery.BigQueryCatalog;
import com.google.zetasql.toolkit.catalog.bigquery.BigQueryService;
import com.google.zetasql.toolkit.options.BigQueryLanguageOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class AntiPatternHelper {
    private static final Logger logger = LoggerFactory.getLogger(AntiPatternHelper.class);
    private String analyzerProject = null;
    private BigQueryAPIResourceProvider resourceProvider;
    private ZetaSQLToolkitAnalyzer analyzer;
    private HashMap<String, Integer> visitorMetricsMap;
    private AnalyzerOptions analyzerOptions;
    private final BigQueryService service;
    private final String project;
    private final LanguageOptions languageOptions;
    private final Boolean useAnalizer;
    public AntiPatternHelper(String project, Boolean useAnalizer) {
        this.project = project;
        this.useAnalizer = useAnalizer;

        this.languageOptions = new LanguageOptions();
        languageOptions.enableMaximumLanguageFeatures();
        languageOptions.setSupportsAllStatementKinds();
        languageOptions.enableReservableKeyword("QUALIFY");

        if (useAnalizer) {
            this.analyzerOptions = new AnalyzerOptions();
            this.analyzer = getAnalyzer(this.analyzerOptions);
            this.service = BigQueryService.buildDefault();
            this.resourceProvider = BigQueryAPIResourceProvider.build(service);
        } else {
            this.service = null;
        }
    }

    public void checkForAntiPatternsInQueryWithParserVisitors(InputQuery inputQuery, List<AntiPatternVisitor> visitorsThatFoundAntiPatterns) {
        List<AntiPatternVisitor> parserVisitorList = getParserVisitorList(inputQuery.getQuery());

        checkForAntiPatternsInQueryWithParserVisitors(inputQuery, visitorsThatFoundAntiPatterns, parserVisitorList);
    }

    public void checkForAntiPatternsInQueryWithParserVisitors(InputQuery inputQuery, List<AntiPatternVisitor> visitorsThatFoundAntiPatterns, List<AntiPatternVisitor> parserVisitorList) {
        if(this.visitorMetricsMap == null) {
            setVisitorMetricsMap(parserVisitorList);
        }

        for (AntiPatternVisitor visitorThatFoundAntiPattern : parserVisitorList) {
            logger.info("Parsing query with id: " + inputQuery.getQueryId() +
                    " for anti-pattern: " + visitorThatFoundAntiPattern.getName());
            ASTNodes.ASTScript parsedQuery = Parser.parseScript( inputQuery.getQuery(), this.languageOptions);
            try{
                parsedQuery.accept((ParseTreeVisitor) visitorThatFoundAntiPattern);
                String result = visitorThatFoundAntiPattern.getResult();
                if(result.length() > 0) {
                    visitorsThatFoundAntiPatterns.add(visitorThatFoundAntiPattern);
                    this.visitorMetricsMap.merge(visitorThatFoundAntiPattern.getName(), 1, Integer::sum);
                }
            } catch (Exception e) {
                logger.error("Error parsing query with id: " + inputQuery.getQueryId() +
                        " for anti-pattern:" + visitorThatFoundAntiPattern.getName());
                logger.error(e.getMessage(), e);
            }
        }
    }

    public void checkForAntiPatternsInQueryWithAnalyzerVisitors(InputQuery inputQuery, List<AntiPatternVisitor> visitorsThatFoundAntiPatterns) {
        String query = inputQuery.getQuery();
        String currentProject;

        if (inputQuery.getProjectId() == null) {
            currentProject = this.project; // Assuming 'this.project' is the default
        } else {
            currentProject = inputQuery.getProjectId();
        }

        // --- 1. Setup Catalog (Consider potential improvements for efficiency if project doesn't change often) ---
        BigQueryCatalog catalog = new BigQueryCatalog(""); // Revisit if default project needed here
        if ((this.analyzerProject == null || !this.analyzerProject.equals(currentProject))) {
            logger.info("Setting up new catalog for project: {}", currentProject);
            this.analyzerProject = currentProject;
            // Ensure 'this.resourceProvider' and 'this.analyzerOptions' are correctly initialized/scoped
            catalog = new BigQueryCatalog(this.analyzerProject, this.resourceProvider);
            try {
                catalog.addAllTablesUsedInQuery(query, this.analyzerOptions);
            } catch (Exception e) {
                logger.error("Error adding tables to catalog for query id: {}. Aborting analysis for this query.", inputQuery.getQueryId(), e);
                return; // Can't proceed without a catalog
            }
        } else {
            logger.info("Reusing existing catalog setup for project: {}", this.analyzerProject);
            // Assuming catalog associated with this.analyzerProject is implicitly available or re-fetched if needed.
            // If catalog state is crucial and might change, you might need to reload tables here too.
            // Recreating for safety in this example:
             catalog = new BigQueryCatalog(this.analyzerProject, this.resourceProvider);
             try {
                catalog.addAllTablesUsedInQuery(query, this.analyzerOptions);
             } catch (Exception e) {
                logger.error("Error adding tables to catalog (reuse path) for query id: {}. Aborting analysis for this query.", inputQuery.getQueryId(), e);
                return;
             }
        }

        // --- 2. Analyze Query ONCE ---
        List<ResolvedNodes.ResolvedStatement> resolvedStatements = new ArrayList<>();
        try {
            logger.info("Analyzing query with id: {}", inputQuery.getQueryId());
            Iterator<ResolvedNodes.ResolvedStatement> statementIterator = this.analyzer.analyzeStatements(query, catalog);
            statementIterator.forEachRemaining(resolvedStatements::add); // Collect statement(s)
        } catch (Exception e) {
            logger.error("Fatal error analyzing query with id: {}. Skipping visitor checks.", inputQuery.getQueryId(), e);
            return; // Cannot proceed if analysis fails
        }

        if (resolvedStatements.isEmpty()) {
            logger.warn("Query analysis did not produce any statements for query id: {}", inputQuery.getQueryId());
            return; // Nothing to visit
        }

        // --- 3. Run Independent Visitors ---
        List<AntiPatternVisitor> independentVisitors = Arrays.asList(
            new JoinOrderVisitor(this.service),
            new PartitionCheckVisitor(this.service)
            // Add any other visitors that DO NOT have dependencies here
        );

        for (AntiPatternVisitor visitor : independentVisitors) {
            runVisitor(visitor, resolvedStatements, inputQuery.getQueryId(), visitorsThatFoundAntiPatterns);
        }

        // --- 4. Run Sequential Clustering Visitors ---

        // 4a. Run the first clustering visitor
        ClusteringCheckVisitor clusteringCheckVisitor = new ClusteringCheckVisitor(this.service);
        runVisitor(clusteringCheckVisitor, resolvedStatements, inputQuery.getQueryId(), visitorsThatFoundAntiPatterns);

        // 4b. Get the clustering info needed for the second visitor
        Map<String, List<String>> clusteringInfo = clusteringCheckVisitor.getClustering(); // Assuming method is getClustering()

        // 4c. Conditionally run the second clustering visitor
        if (!clusteringCheckVisitor.getContainsUnclusteredTables()) {
            logger.info("Found clustered tables. Running ClusteringKeysUsedVisitor for query id: {}", inputQuery.getQueryId());
            ClusteringKeysUsedVisitor clusteringKeysUsedVisitor = new ClusteringKeysUsedVisitor(clusteringInfo);

            // Run the visitor (AST traversal)
            try {
                for (ResolvedNodes.ResolvedStatement statement : resolvedStatements) {
                    // Ensure casting is correct based on your interfaces/inheritance
                    statement.accept((ResolvedNodes.Visitor) clusteringKeysUsedVisitor);
                }

                // IMPORTANT: Analyze usage AFTER traversal
                clusteringKeysUsedVisitor.analyzeKeyUsage();

                // Get result and add to list if applicable
                String usageResult = clusteringKeysUsedVisitor.getResult();
                if (usageResult != null && !usageResult.isEmpty()) {
                    logger.info("Anti-pattern found by {}: {}", clusteringKeysUsedVisitor.getName(), usageResult);
                    visitorsThatFoundAntiPatterns.add(clusteringKeysUsedVisitor);
                    // Update metrics if needed
                    this.visitorMetricsMap.computeIfAbsent(clusteringKeysUsedVisitor.getName(), k -> 0);
                    this.visitorMetricsMap.merge(clusteringKeysUsedVisitor.getName(), 1, Integer::sum);
                } else {
                     logger.info("No unused clustering keys found by {} for query id: {}", clusteringKeysUsedVisitor.getName(), inputQuery.getQueryId());
                }

            } catch (ClassCastException cce) {
                 logger.error("Visitor {} does not seem to be a ResolvedNodes.Visitor. Query id: {}", clusteringKeysUsedVisitor.getName(), inputQuery.getQueryId(), cce);
            } catch (Exception e) {
                logger.error("Error running visitor {} for query id: {}", clusteringKeysUsedVisitor.getName(), inputQuery.getQueryId(), e);
            }
        } else {
            logger.info("Skipping ClusteringKeysUsedVisitor for query id: {} as no clustered tables were detected by ClusteringCheckVisitor.", inputQuery.getQueryId());
        }
    }

    /**
     * Helper method to run a visitor on all statements and handle results/errors.
     * @param visitor The AntiPatternVisitor instance (must also be a ResolvedNodes.Visitor)
     * @param statements The list of resolved statements from the analyzer.
     * @param queryId The ID of the query being analyzed.
     * @param visitorsThatFoundAntiPatterns The output list to add visitors to if they find anti-patterns.
     */
    private void runVisitor(AntiPatternVisitor visitor, List<ResolvedNodes.ResolvedStatement> statements, String queryId, List<AntiPatternVisitor> visitorsThatFoundAntiPatterns) {
        // Update metrics map (assuming it should be updated for every visitor run attempt)
        this.visitorMetricsMap.computeIfAbsent(visitor.getName(), k -> 0);
        this.visitorMetricsMap.merge(visitor.getName(), 1, Integer::sum);

        try {
            logger.info("Running visitor: {} for query id: {}", visitor.getName(), queryId);
            // Cast to the base ResolvedNodes.Visitor for the accept method
            ResolvedNodes.Visitor resolvedVisitor = (ResolvedNodes.Visitor) visitor;
            for (ResolvedNodes.ResolvedStatement statement : statements) {
                statement.accept(resolvedVisitor);
            }

            // Get the result AFTER visiting all statements
            String result = visitor.getResult();
            if (result != null && !result.isEmpty()) {
                logger.info("Anti-pattern found by {}: {}", visitor.getName(), result);
                visitorsThatFoundAntiPatterns.add(visitor);
            } else {
                 logger.info("No anti-patterns found by {} for query id: {}", visitor.getName(), queryId);
            }
        } catch (ClassCastException cce) {
            // Handle cases where an AntiPatternVisitor might not be a ResolvedNodes.Visitor
            logger.error("Visitor {} cannot be cast to ResolvedNodes.Visitor. Query id: {}", visitor.getName(), queryId, cce);
        } catch (Exception e) {
            // Catch potential errors during the visit or getResult methods
            logger.error("Error running visitor {} for query id: {}", visitor.getName(), queryId, e);
        }
    }

    // THE ORDER HERE MATTERS
    // this is also the order in which the rewrites get applied
    public List<AntiPatternVisitor> getParserVisitorList(String query) {
        return new ArrayList<>(Arrays.asList(
                new IdentifySimpleSelectStarVisitor(),
                new IdentifyInSubqueryWithoutAggVisitor(query),
                new IdentifyDynamicPredicateVisitor(query),
                new IdentifyOrderByWithoutLimitVisitor(query),
                new IdentifyRegexpContainsVisitor(query),
                new IdentifyCTEsEvalMultipleTimesVisitor(query),
                new IdentifyLatestRecordVisitor(query),
                new IdentifyWhereOrderVisitor(query),
                new IdentifyMissingDropStatementVisitor(query),
                new IdentifyDroppedPersistentTableVisitor(query)

        ));
    }

    public String getProject() {
        return project;
    }

    public Boolean getUseAnalizer() {
        return useAnalizer;
    }

    private void setVisitorMetricsMap(List<AntiPatternVisitor> parserVisitorList ) {
        this.visitorMetricsMap = new HashMap<>();
        parserVisitorList.stream().forEach(visitor -> this.visitorMetricsMap.put(visitor.getName(), 0));
    }

    private ZetaSQLToolkitAnalyzer getAnalyzer(AnalyzerOptions options) {
        LanguageOptions languageOptions = BigQueryLanguageOptions.get().enableMaximumLanguageFeatures();
        languageOptions.setSupportsAllStatementKinds();
        options.setLanguageOptions(languageOptions);
        options.setCreateNewColumnForEachProjectedOutput(true);
        return new ZetaSQLToolkitAnalyzer(options);
    }
}
