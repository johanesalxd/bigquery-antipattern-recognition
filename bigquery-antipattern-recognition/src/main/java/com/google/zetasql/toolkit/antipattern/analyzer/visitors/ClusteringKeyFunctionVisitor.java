package com.google.zetasql.toolkit.antipattern.analyzer.visitors; // Adjust package if needed

import com.google.zetasql.resolvedast.ResolvedNodes;
import com.google.zetasql.resolvedast.ResolvedNodes.ResolvedFilterScan;
import com.google.zetasql.resolvedast.ResolvedNodes.ResolvedTableScan;
import com.google.zetasql.toolkit.antipattern.AntiPatternVisitor;
import com.google.zetasql.toolkit.catalog.bigquery.BigQueryService; // Not used directly here, but needed by caller

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Visitor to identify cases where functions are applied to clustering key columns
 * within filter predicates, potentially hindering cluster pruning.
 * Requires a map of clustered tables to their keys as input.
 */
public class ClusteringKeyFunctionVisitor extends ResolvedNodes.Visitor implements AntiPatternVisitor {

    public static final String NAME = "FunctionOnClusteringKeyCheck";
    // Recommendation message is generated dynamically by the helper visitor

    // Input: Map of clustered_table_name -> List<clustering_key_columns>
    private final Map<String, List<String>> clusteringFields;

    // Output: Set of unique warning messages found
    private final Set<String> findings = new HashSet<>();

    public ClusteringKeyFunctionVisitor(Map<String, List<String>> clusteringFields) {
        // Defensive copy might be good practice
        this.clusteringFields = new HashMap<>(clusteringFields);
    }

    @Override
    public String getName() {
        return NAME;
    }

    /**
     * Returns a string containing warnings about functions applied to clustering keys
     * within filter predicates, or an empty string if none were found.
     */
    @Override
    public String getResult() {
        if (findings.isEmpty()) {
            return "";
        }
        // Sort findings for consistent output order
        return findings.stream().sorted().collect(Collectors.joining("\n"));
    }

    /**
     * Visits ResolvedFilterScan nodes to analyze the filter expression for
     * functions applied to clustering keys.
     * @param node The ResolvedFilterScan node from the ZetaSQL AST.
     */
    @Override
    public void visit(ResolvedFilterScan node) {
        // 1. Identify Input Table - Focusing on single ResolvedTableScan input for simplicity
        String targetTableName = null;
        List<String> keysForTable = null;

        if (node.getInputScan() instanceof ResolvedTableScan) {
            ResolvedTableScan inputTableScan = (ResolvedTableScan) node.getInputScan();
            // Assuming getFullName provides format "project.dataset.table"
            targetTableName = inputTableScan.getTable().getFullName();
            keysForTable = clusteringFields.get(targetTableName);
        }

        // 2. Proceed only if it's a single table scan with known clustering keys
        if (targetTableName != null && keysForTable != null && !keysForTable.isEmpty()) {

            // 3. Use the helper visitor to scan the filter expression for the anti-pattern
            if (node.getFilterExpr() != null) {
                FunctionOnClusteringColumnFinder functionFinder =
                    new FunctionOnClusteringColumnFinder(targetTableName, keysForTable, findings);
                node.getFilterExpr().accept(functionFinder);
            }
        }

        // 4. Continue traversal for nested filters or other structures
        super.visit(node);
    }

    // NOTE: This visitor primarily checks filters directly applied to a table scan.
    // Filters applied after joins might require analyzing ResolvedFilterScan nodes
    // higher up the tree and more complex logic to determine the relevant table(s).
    // The heuristic used in the helper visitor for table name resolution is a limitation.
}  
