package com.google.zetasql.toolkit.antipattern.analyzer.visitors;

import com.google.zetasql.resolvedast.ResolvedColumn;
import com.google.zetasql.resolvedast.ResolvedNodes.ResolvedColumnRef;
import com.google.zetasql.resolvedast.ResolvedNodes.ResolvedExpr;
import com.google.zetasql.resolvedast.ResolvedNodes;
import com.google.zetasql.resolvedast.ResolvedNodes.ResolvedFunctionCall;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Helper visitor to find instances where a clustering key column is used
 * inside a function call, which itself is an argument to a comparison function.
 * NOTE: Relies on heuristics to map columns back to the target table, which may
 * not be accurate in complex queries (e.g., joins, subqueries).
 */
class FunctionOnClusteringColumnFinder extends ResolvedNodes.Visitor {

    private final String targetTableName; // Expected format: project.dataset.table
    private final List<String> clusteringKeys;
    private final Set<String> findings; // Set of warning messages generated

    // Common comparison functions where cluster pruning might be affected
    // Expand this list based on functions commonly used in predicates.
    private static final Set<String> COMPARISON_FUNCTIONS = new HashSet<>(Arrays.asList(
        "$equal", "$less", "$less_or_equal", "$greater", "$greater_or_equal",
        "$not_equal", "$like", "$in", "$between"
        // Consider adding string functions often used in comparisons like STARTS_WITH, ENDS_WITH if applicable
    ));

    FunctionOnClusteringColumnFinder(String targetTableName, List<String> clusteringKeys, Set<String> findings) {
        this.targetTableName = targetTableName;
        this.clusteringKeys = clusteringKeys;
        this.findings = findings;
    }

    @Override
    public void visit(ResolvedFunctionCall outerFuncCall) {
        // Is this function a comparison operator we care about?
        if (COMPARISON_FUNCTIONS.contains(outerFuncCall.getFunction().getName())) {

            // Check each argument of the comparison function
            for (ResolvedExpr outerArg : outerFuncCall.getArgumentList()) {

                // Is this argument *itself* a function call?
                if (outerArg instanceof ResolvedFunctionCall) {
                    ResolvedFunctionCall innerFuncCall = (ResolvedFunctionCall) outerArg;

                    // Check arguments of the inner function call
                    for (ResolvedExpr innerArg : innerFuncCall.getArgumentList()) {

                        // Is this inner argument a column reference?
                        if (innerArg instanceof ResolvedColumnRef) {
                            ResolvedColumnRef colRef = (ResolvedColumnRef) innerArg;
                            String colName = colRef.getColumn().getName();

                            // Heuristically check if this column likely belongs to the target table
                            // and is one of its clustering keys.
                            // !! WARNING: getTableNameFromColumnHeuristic is a placeholder !!
                            // !! A robust implementation needs reliable column provenance tracking. !!
                            String colTableName = getTableNameFromColumnHeuristic(colRef.getColumn());

                            if (targetTableName.equals(colTableName) && clusteringKeys.contains(colName)) {
                                // Found the anti-pattern!
                                String warning = String.format(
                                    "Potential anti-pattern: Function '%s' used on clustering key '%s' of table '%s' inside a predicate (%s). " +
                                    "This often prevents cluster pruning. Consider applying functions to constants/parameters instead (e.g., `key = FUNC(value)` instead of `FUNC(key) = value`).",
                                    innerFuncCall.getFunction().getName(),
                                    colName,
                                    targetTableName,
                                    outerFuncCall.getFunction().getName() // Name of the comparison func
                                );
                                findings.add(warning);
                                // Found it for this colRef, no need to check deeper within *this specific* innerFuncCall's args
                                // But we must continue checking other args of the outerFuncCall and other functions.
                            }
                        }
                        // Optimization: If innerArg is also a function, we could recurse
                        // else if (innerArg instanceof ResolvedFunctionCall) { innerArg.accept(this); }
                        // But the simple check above handles the direct FUNC(COLUMN_REF) case.
                    }
                }
            }
        }

        // IMPORTANT: Always continue traversal to find other comparison functions
        // deeper in the expression tree or other branches of an AND/OR.
        super.visit(outerFuncCall);
    }

    /**
     * Placeholder/Heuristic for getting the table name associated with a ResolvedColumn.
     * A robust implementation requires mapping ResolvedColumn IDs back to their
     * originating ResolvedTableScan nodes, potentially via a pre-analysis pass or
     * passing context down the visitor stack. This simple version ASSUMES the column
     * must belong to the target table being filtered if its name matches a clustering key.
     * This assumption can be incorrect.
     *
     * @param column The ResolvedColumn.
     * @return The assumed table name (currently just returns targetTableName).
     */
    private String getTableNameFromColumnHeuristic(ResolvedColumn column) {
        // System.err.println("WARNING: Using simplified heuristic to map column to table in FunctionOnClusteringColumnFinder.");
        // In the context of visitResolvedFilterScan on a single table, assuming the column
        // belongs to that table if the name matches is the simplest approach, but fragile.
        return targetTableName;
    }
}