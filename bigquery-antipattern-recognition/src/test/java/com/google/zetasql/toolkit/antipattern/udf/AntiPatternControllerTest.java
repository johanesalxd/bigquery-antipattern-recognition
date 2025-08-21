package com.google.zetasql.toolkit.antipattern.udf;

import org.junit.Test;
import org.springframework.boot.test.context.SpringBootTest;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.zetasql.toolkit.antipattern.controller.AntiPatternController;
import com.google.zetasql.toolkit.antipattern.models.BigQueryRemoteFnRequest;
import com.google.zetasql.toolkit.antipattern.models.BigQueryRemoteFnResponse;
import com.google.zetasql.toolkit.antipattern.models.BigQueryRemoteFnResult;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

@SpringBootTest
public class AntiPatternControllerTest {

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final AntiPatternController antiPatternController = new AntiPatternController();

    @Test
    public void testRequestWithNoAntipattern() throws Exception {
        BigQueryRemoteFnRequest request = createRequest(List.of("SELECT id FROM dataset.table"));

        ObjectNode response = antiPatternController.analyzeQueries(request);
        BigQueryRemoteFnResponse responseObj = objectMapper.convertValue(response.get("replies").get(0),
                BigQueryRemoteFnResponse.class);

        List<BigQueryRemoteFnResult> results = responseObj.getAntipatterns();
        assertEquals("None", results.get(0).getName());

    }

    @Test
    public void testRequestWithOneAntipattern() throws Exception {
        BigQueryRemoteFnRequest request = createRequest(List.of("SELECT * FROM dataset.table"));

        ObjectNode response = antiPatternController.analyzeQueries(request);
        BigQueryRemoteFnResponse responseObj = objectMapper.convertValue(response.get("replies").get(0),
                BigQueryRemoteFnResponse.class);

        List<BigQueryRemoteFnResult> results = responseObj.getAntipatterns();
        assertEquals("SimpleSelectStar", results.get(0).getName());

    }

    @Test
    public void testRequestWithTwoAntipatterns() throws Exception {
        BigQueryRemoteFnRequest request = createRequest(List.of("SELECT * FROM dataset.table ORDER BY id"));

        ObjectNode response = antiPatternController.analyzeQueries(request);
        BigQueryRemoteFnResponse responseObj = objectMapper.convertValue(response.get("replies").get(0),
                BigQueryRemoteFnResponse.class);

        List<BigQueryRemoteFnResult> results = responseObj.getAntipatterns();
        assertEquals("SimpleSelectStar", results.get(0).getName());
        assertEquals("OrderByWithoutLimit", results.get(1).getName());

    }

    @Test
    public void testRequestWithTwoQueriesWithAntipatterns() throws Exception {
        BigQueryRemoteFnRequest request = createRequest(
                List.of("SELECT * FROM dataset.table", "SELECT id FROM dataset.table ORDER BY id"));

        ObjectNode response = antiPatternController.analyzeQueries(request);
        BigQueryRemoteFnResponse responseObj1 = objectMapper.convertValue(response.get("replies").get(0),
                BigQueryRemoteFnResponse.class);
        BigQueryRemoteFnResponse responseObj2 = objectMapper.convertValue(response.get("replies").get(1),
                BigQueryRemoteFnResponse.class);

        List<BigQueryRemoteFnResult> results1 = responseObj1.getAntipatterns();
        List<BigQueryRemoteFnResult> results2 = responseObj2.getAntipatterns();

        assertEquals("SimpleSelectStar", results1.get(0).getName());
        assertEquals("OrderByWithoutLimit", results2.get(0).getName());

    }

    @Test
    public void testRequestWithInvalidQuery() throws Exception {
        BigQueryRemoteFnRequest request = createRequest(
                List.of("123"));

        ObjectNode response = antiPatternController.analyzeQueries(request);
        BigQueryRemoteFnResponse responseObj = objectMapper.convertValue(response.get("replies").get(0),
                BigQueryRemoteFnResponse.class);

        String results = responseObj.getErrorMessage();

        assertEquals("Syntax error: Expected end of input but got integer literal \"123\" [at 1:1]", results);

    }

    @Test
    public void testRequestWithAntipatternAndInvalidQuery() throws Exception {
        BigQueryRemoteFnRequest request = createRequest(
                List.of("SELECT * FROM dataset.table", "123"));

        ObjectNode response = antiPatternController.analyzeQueries(request);
        BigQueryRemoteFnResponse responseObj1 = objectMapper.convertValue(response.get("replies").get(0),
                BigQueryRemoteFnResponse.class);
        BigQueryRemoteFnResponse responseObj2 = objectMapper.convertValue(response.get("replies").get(1),
                BigQueryRemoteFnResponse.class);

        List<BigQueryRemoteFnResult> results1 = responseObj1.getAntipatterns();

        String results2 = responseObj2.getErrorMessage();
        System.out.println("+++++");
        System.out.println(results2);

        assertEquals("SimpleSelectStar", results1.get(0).getName());
        assertEquals("Syntax error: Expected end of input but got integer literal \"123\" [at 1:1]", results2);

    }

    private BigQueryRemoteFnRequest createRequest(List<String> queries) {
        List<JsonNode> calls = new ArrayList<>();

        for (String query : queries) {
            ArrayNode queryArray = objectMapper.createArrayNode();
            queryArray.add(query);
            calls.add(queryArray);
        }

        return new BigQueryRemoteFnRequest(
            "requestId",
            "caller",
            "sessionUser",
            new HashMap<>(),
            calls
        );
    }

    // Tests for the AI rewrite endpoint

    @Test
    public void testRewriteEndpointWithNoAntipatterns() throws Exception {
        BigQueryRemoteFnRequest request = createRequest(List.of("SELECT id FROM dataset.table"));

        ObjectNode response = antiPatternController.analyzeAndRewriteQueries(request);
        BigQueryRemoteFnResponse responseObj = objectMapper.convertValue(response.get("replies").get(0),
                BigQueryRemoteFnResponse.class);

        assertEquals("None", responseObj.getAntipatterns().get(0).getName());
        assertNull(responseObj.getOptimized_sql()); // No optimization when no antipatterns
    }

    @Test
    public void testRewriteEndpointWithAntipatterns() throws Exception {
        BigQueryRemoteFnRequest request = createRequest(List.of("SELECT * FROM dataset.table"));

        ObjectNode response = antiPatternController.analyzeAndRewriteQueries(request);
        BigQueryRemoteFnResponse responseObj = objectMapper.convertValue(response.get("replies").get(0),
                BigQueryRemoteFnResponse.class);

        assertEquals("SimpleSelectStar", responseObj.getAntipatterns().get(0).getName());
        // optimized_sql will be null in test environment (no PROJECT_ID set)
    }

    @Test
    public void testStandardVsRewriteEndpoints() throws Exception {
        BigQueryRemoteFnRequest request = createRequest(List.of("SELECT * FROM dataset.table"));

        // Standard endpoint should not include optimized_sql
        ObjectNode standardResponse = antiPatternController.analyzeQueries(request);
        BigQueryRemoteFnResponse standardObj = objectMapper.convertValue(standardResponse.get("replies").get(0),
                BigQueryRemoteFnResponse.class);
        assertNull(standardObj.getOptimized_sql());

        // Rewrite endpoint includes optimized_sql field (even if null)
        ObjectNode rewriteResponse = antiPatternController.analyzeAndRewriteQueries(request);
        BigQueryRemoteFnResponse rewriteObj = objectMapper.convertValue(rewriteResponse.get("replies").get(0),
                BigQueryRemoteFnResponse.class);
        // Field exists but is null due to missing PROJECT_ID in test environment
        assertNotNull(rewriteObj.getAntipatterns());
    }

}
