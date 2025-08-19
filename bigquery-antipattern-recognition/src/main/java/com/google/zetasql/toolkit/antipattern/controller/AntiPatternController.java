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

package com.google.zetasql.toolkit.antipattern.controller;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.zetasql.toolkit.antipattern.AntiPatternVisitor;
import com.google.zetasql.toolkit.antipattern.cmd.InputQuery;
import com.google.zetasql.toolkit.antipattern.models.BigQueryRemoteFnRequest;
import com.google.zetasql.toolkit.antipattern.models.BigQueryRemoteFnResponse;
import com.google.zetasql.toolkit.antipattern.models.BigQueryRemoteFnResult;
import com.google.zetasql.toolkit.antipattern.rewriter.gemini.GeminiRewriter;
import com.google.zetasql.toolkit.antipattern.util.AntiPatternHelper;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

@RestController
public class AntiPatternController {

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final String projectId;

    public AntiPatternController() {
        this.projectId = System.getenv("PROJECT_ID");
    }

    @PostMapping("/")
    public ObjectNode analyzeQueries(@RequestBody BigQueryRemoteFnRequest request) {
        ArrayNode replies = objectMapper.createArrayNode();

        for (JsonNode call : request.getCalls()) {
            BigQueryRemoteFnResponse queryResponse = analyzeSingleQuery(call, false);
            ObjectNode resultNode = objectMapper.valueToTree(queryResponse);
            replies.add(resultNode);
        }

        ObjectNode finalResponse = objectMapper.createObjectNode();
        finalResponse.set("replies", replies);
        return finalResponse;
    }

    @PostMapping("/rewrite")
    public ObjectNode analyzeAndRewriteQueries(@RequestBody BigQueryRemoteFnRequest request) {
        ArrayNode replies = objectMapper.createArrayNode();

        for (JsonNode call : request.getCalls()) {
            BigQueryRemoteFnResponse queryResponse = analyzeSingleQuery(call, true);
            ObjectNode resultNode = objectMapper.valueToTree(queryResponse);
            replies.add(resultNode);
        }

        ObjectNode finalResponse = objectMapper.createObjectNode();
        finalResponse.set("replies", replies);
        return finalResponse;
    }

    private BigQueryRemoteFnResponse analyzeSingleQuery(JsonNode call, boolean enableRewrite) {
        try {
            InputQuery inputQuery = new InputQuery(call.get(0).asText(), "query provided by UDF:");
            List<AntiPatternVisitor> visitors = findAntiPatterns(inputQuery);
            List<BigQueryRemoteFnResult> formattedAntiPatterns = new ArrayList<>();

            if (visitors.isEmpty()) {
                formattedAntiPatterns.add(new BigQueryRemoteFnResult("None", "No antipatterns found"));
            } else {
                formattedAntiPatterns = BigQueryRemoteFnResponse.formatAntiPatterns(visitors);
            }

            String optimizedSql = null;
            if (enableRewrite && !visitors.isEmpty()) {
                optimizedSql = rewriteQueryWithAI(inputQuery.getQuery(), visitors);
            }

            return new BigQueryRemoteFnResponse(formattedAntiPatterns, optimizedSql, null);
        } catch (Exception e) {
            return new BigQueryRemoteFnResponse(null, null, e.getMessage());
        }
    }

    private String rewriteQueryWithAI(String originalQuery, List<AntiPatternVisitor> antiPatterns) {
        try {
            if (projectId == null || projectId.isEmpty()) {
                throw new IllegalStateException("PROJECT_ID environment variable is not set");
            }

            // Create a temporary InputQuery for rewriting
            InputQuery inputQuery = new InputQuery(originalQuery, "query for AI rewrite");
            AntiPatternHelper antiPatternHelper = new AntiPatternHelper(projectId, false);

            // Use GeminiRewriter static method to rewrite the SQL
            GeminiRewriter.rewriteSQL(inputQuery, antiPatterns, antiPatternHelper, 3, true);

            // Return the optimized query if available, otherwise null
            return inputQuery.getOptimizedQuery();
        } catch (Exception e) {
            // Log the error but don't fail the entire response
            System.err.println("AI rewrite failed: " + e.getMessage());
            return null;
        }
    }

    private List<AntiPatternVisitor> findAntiPatterns(InputQuery inputQuery) {
        List<AntiPatternVisitor> visitors = new ArrayList<>();
        AntiPatternHelper antiPatternHelper = new AntiPatternHelper(projectId, false);
        antiPatternHelper.checkForAntiPatternsInQueryWithParserVisitors(inputQuery, visitors);
        return visitors;
    }

    public static List<BigQueryRemoteFnResult> formatAntiPatterns(List<AntiPatternVisitor> visitors) {
        return visitors.stream()
                .map(visitor -> new BigQueryRemoteFnResult(visitor.getName(), visitor.getResult()))
                .collect(Collectors.toList());
    }
}
