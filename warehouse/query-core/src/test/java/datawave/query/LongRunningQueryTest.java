package datawave.query;

import com.google.common.collect.Sets;
import datawave.helpers.PrintUtility;
import datawave.marking.MarkingFunctions;
import datawave.query.tables.ShardQueryLogic;
import datawave.query.util.DateIndexHelperFactory;
import datawave.query.util.MetadataHelperFactory;
import datawave.query.util.VisibilityWiseGuysIngest;
import datawave.security.authorization.DatawavePrincipal;
import datawave.security.authorization.DatawaveUser;
import datawave.security.authorization.SubjectIssuerDNPair;
import datawave.security.util.DnUtils;
import datawave.util.TableName;
import datawave.webservice.common.connection.AccumuloConnectionFactory;
import datawave.webservice.query.QueryImpl;
import datawave.webservice.query.cache.QueryMetricFactoryImpl;
import datawave.webservice.query.cache.ResultsPage;
import datawave.webservice.query.configuration.GenericQueryConfiguration;
import datawave.webservice.query.result.event.DefaultResponseObjectFactory;
import datawave.webservice.query.runner.RunningQuery;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.log4j.Logger;
import org.junit.Before;
import org.junit.Test;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class LongRunningQueryTest {
    
    // variables common to all current tests
    private SubjectIssuerDNPair userDN = SubjectIssuerDNPair.of("userDn", "issuerDn");
    private static Authorizations auths = new Authorizations("ALL", "E", "I");
    private final DateFormat format = new SimpleDateFormat("yyyyMMdd");
    private static MockAccumuloRecordWriter recordWriter;
    private DatawavePrincipal datawavePrincipal;
    private static final Logger log = Logger.getLogger(LongRunningQueryTest.class);
    private static Connector connector = null;
    
    @Before
    public void setup() throws Exception {
        
        System.setProperty(DnUtils.NpeUtils.NPE_OU_PROPERTY, "iamnotaperson");
        System.setProperty("dw.metadatahelper.all.auths", "A,B,C,D,E,I");
        System.setProperty("file.encoding", "UTF-8");
        DatawaveUser user = new DatawaveUser(userDN, DatawaveUser.UserType.USER, Sets.newHashSet(auths.toString().split(",")), null, null, -1L);
        datawavePrincipal = new DatawavePrincipal((Collections.singleton(user)));
        
        TimeZone.setDefault(TimeZone.getTimeZone("GMT"));
        QueryTestTableHelper testTableHelper = new QueryTestTableHelper(LongRunningQueryTest.class.toString(), log);
        recordWriter = new MockAccumuloRecordWriter();
        testTableHelper.configureTables(recordWriter);
        connector = testTableHelper.connector;
        
        // Load data for the test
        VisibilityWiseGuysIngest.writeItAll(connector, VisibilityWiseGuysIngest.WhatKindaRange.DOCUMENT);
        PrintUtility.printTable(connector, auths, TableName.SHARD);
        PrintUtility.printTable(connector, auths, TableName.SHARD_INDEX);
        PrintUtility.printTable(connector, auths, QueryTestTableHelper.MODEL_TABLE_NAME);
    }
    
    /**
     * A groupBy query is one type of query that is allowed to be "long running", so that type of query is used in this test.
     *
     * A long running query will return a ResultsPage with zero results if it has not completed within the query execution page timeout. This test expects at
     * least 2 pages (the exact number will depend on cpu speed). All but the lsat page should have 0 results and be marked as PARTIAL. The last page should
     * have 8 results and have a status of COMPLETE.
     *
     * @throws Exception
     */
    @Test
    public void testAllowLongRunningQueryWithShardQueryLogic() throws Exception {
        
        Map<String,String> extraParameters = new HashMap<>();
        extraParameters.put("group.fields", "AGE,$GENDER");
        extraParameters.put("group.fields.batch.size", "6");
        
        String queryStr = "UUID =~ '^[CS].*'";
        Date startDate = format.parse("20091231");
        Date endDate = format.parse("20150101");
        QueryImpl query = new QueryImpl();
        query.setQuery(queryStr);
        query.setBeginDate(startDate);
        query.setEndDate(endDate);
        query.setQueryAuthorizations(auths.serialize());
        query.setColumnVisibility("A&E&I");
        query.setPagesize(Integer.MAX_VALUE);
        query.setParameters(extraParameters);
        query.setId(UUID.randomUUID());
        
        ShardQueryLogic logic = new ShardQueryLogic();
        logic.setIncludeGroupingContext(true);
        logic.setIncludeDataTypeAsField(true);
        logic.setMarkingFunctions(new MarkingFunctions.Default());
        logic.setMetadataHelperFactory(new MetadataHelperFactory());
        logic.setDateIndexHelperFactory(new DateIndexHelperFactory());
        logic.setResponseObjectFactory(new DefaultResponseObjectFactory());
        // this parameter is what makes the query long running. Failing to set this will let it default to 50 minutes
        // (and not the 200 milliseconds that it is set to) which will return only 1 page of 8 results, thereby failing this test.
        // the smaller this timeout, the more pages of results that will be returned.
        logic.setQueryExecutionForPageTimeout(200);
        logic.setLongRunningQuery(true);
        
        GenericQueryConfiguration config = logic.initialize(connector, query, Collections.singleton(auths));
        logic.setupQuery(config);
        
        RunningQuery runningQuery = new RunningQuery(connector, AccumuloConnectionFactory.Priority.NORMAL, logic, query, "", datawavePrincipal,
                        new QueryMetricFactoryImpl());
        List<ResultsPage> pages = new ArrayList<>();
        
        ResultsPage page = runningQuery.next();
        pages.add(page);
        // guarantee the need for at least a second page. (make the wait slightly longer than the page timeout is set to)
        try {
            Thread.sleep(250);
        } catch (InterruptedException e) {
            
        }
        
        while (page.getStatus() != ResultsPage.Status.COMPLETE) {
            page = runningQuery.next();
            pages.add(page);
        }
        
        // There should be at least 2 pages, more depending on cpu speed.
        assertTrue(pages.size() > 1);
        for (int i = 0; i < pages.size() - 1; ++i) {
            // check every page but the last one for 0 results and PARTIAL status
            assertEquals(0, pages.get(i).getResults().size());
            assertEquals(ResultsPage.Status.PARTIAL, pages.get(i).getStatus());
        }
        // check the last page for COMPLETE status and that the total number of results is 8
        assertEquals(8, pages.get(pages.size() - 1).getResults().size());
        assertEquals(ResultsPage.Status.COMPLETE, pages.get(pages.size() - 1).getStatus());
    }
    
    /**
     * Tests that the code path that allows long running queries does not interfere or create a never ending query if a query legitimately doesn't have results.
     * Set the queryExecutionForPageTimeout to an extremely small value (10ms) so that we still hit the timeout per page, even though there will be no results.
     * should have 1 - n pages with 0 results and a status of PARTIAL, but the last page should have 0 results and a status of NONE
     *
     * @throws Exception
     */
    @Test
    public void testAllowLongRunningQueryOnNoResultsc() throws Exception {
        
        Map<String,String> extraParameters = new HashMap<>();
        
        extraParameters.put("group.fields", "AGE,$GENDER");
        extraParameters.put("group.fields.batch.size", "6");
        
        // There should be no results for this query
        String queryStr = "UUID =~ '^[NAN].*'";
        Date startDate = format.parse("20091231");
        Date endDate = format.parse("20150101");
        QueryImpl query = new QueryImpl();
        query.setQuery(queryStr);
        query.setBeginDate(startDate);
        query.setEndDate(endDate);
        query.setQueryAuthorizations(auths.serialize());
        query.setColumnVisibility("A&E&I");
        query.setPagesize(Integer.MAX_VALUE);
        query.setParameters(extraParameters);
        query.setId(UUID.randomUUID());
        
        ShardQueryLogic logic = new ShardQueryLogic();
        logic.setIncludeGroupingContext(true);
        logic.setIncludeDataTypeAsField(true);
        logic.setMarkingFunctions(new MarkingFunctions.Default());
        logic.setMetadataHelperFactory(new MetadataHelperFactory());
        logic.setDateIndexHelperFactory(new DateIndexHelperFactory());
        logic.setResponseObjectFactory(new DefaultResponseObjectFactory());
        // this parameter is what makes the query long running. Failing to set this will let it default to 50 minutes
        // (and not the 10 milliseconds that it is set to) Set to just 10 ms because since there will be no results,
        // we have to make sure we hit the timeout before no results are returned.
        logic.setQueryExecutionForPageTimeout(10);
        logic.setLongRunningQuery(true);
        
        GenericQueryConfiguration config = logic.initialize(connector, query, Collections.singleton(auths));
        logic.setupQuery(config);
        
        RunningQuery runningQuery = new RunningQuery(connector, AccumuloConnectionFactory.Priority.NORMAL, logic, query, "", datawavePrincipal,
                        new QueryMetricFactoryImpl());
        List<ResultsPage> pages = new ArrayList<>();
        
        ResultsPage page = runningQuery.next();
        pages.add(page);
        
        // guarantee the need for at least a second page.
        try {
            Thread.sleep(250);
        } catch (InterruptedException e) {
            
        }
        
        while (page.getStatus() != ResultsPage.Status.NONE) {
            page = runningQuery.next();
            pages.add(page);
        }
        
        // There should be at least 2 pages, more depending on cpu speed.
        assertTrue(pages.size() > 1);
        for (int i = 0; i < pages.size() - 1; ++i) {
            // check every page but the last one for 0 results and PARTIAL status
            assertEquals(0, pages.get(i).getResults().size());
            assertEquals(ResultsPage.Status.PARTIAL, pages.get(i).getStatus());
        }
        // check the last page for COMPLETE status and that the total number of results is 8
        assertEquals(0, pages.get(pages.size() - 1).getResults().size());
        assertEquals(ResultsPage.Status.NONE, pages.get(pages.size() - 1).getStatus());
    }
    
    /**
     * Tests that having results returned over multiple pages (by setting the page size to be less than the number of results that will be returned) behaves
     * even with long running queries exceeding their execution timeout for a page. Should get at least 1 page of 0 results and a PARTIAL status, but could be
     * more depending on cpu speed due hitting the execution timeout. The last two pages should have 4 results each, but the last page should have a status of
     * COMPLETE, and the next to last page should have a status of PARTIAL.
     *
     * @throws Exception
     */
    @Test
    public void testAllowLongRunningQueryWithSmallPageSize() throws Exception {
        
        Map<String,String> extraParameters = new HashMap<>();
        
        extraParameters.put("group.fields", "AGE,$GENDER");
        extraParameters.put("group.fields.batch.size", "6");
        
        String queryStr = "UUID =~ '^[CS].*'";
        Date startDate = format.parse("20091231");
        Date endDate = format.parse("20150101");
        QueryImpl query = new QueryImpl();
        query.setQuery(queryStr);
        query.setBeginDate(startDate);
        query.setEndDate(endDate);
        query.setQueryAuthorizations(auths.serialize());
        query.setColumnVisibility("A&E&I");
        
        query.setPagesize(4);
        query.setParameters(extraParameters);
        query.setId(UUID.randomUUID());
        
        ShardQueryLogic logic = new ShardQueryLogic();
        logic.setIncludeGroupingContext(true);
        logic.setIncludeDataTypeAsField(true);
        logic.setMarkingFunctions(new MarkingFunctions.Default());
        logic.setMetadataHelperFactory(new MetadataHelperFactory());
        logic.setDateIndexHelperFactory(new DateIndexHelperFactory());
        logic.setResponseObjectFactory(new DefaultResponseObjectFactory());
        // this parameter is what makes the query long running. Failing to set this will let it default to 50 minutes
        // (and not the 500 milliseconds that it is set to) which will return only 1 page of 8 results, thereby failing this test.
        // the smaller this timeout, the more pages of results that will be returned.
        logic.setQueryExecutionForPageTimeout(10);
        logic.setLongRunningQuery(true);
        // We expect 8 results, so this allows us to test getting those results over 2 pages
        logic.setMaxPageSize(4);
        
        GenericQueryConfiguration config = logic.initialize(connector, query, Collections.singleton(auths));
        logic.setupQuery(config);
        
        RunningQuery runningQuery = new RunningQuery(connector, AccumuloConnectionFactory.Priority.NORMAL, logic, query, "", datawavePrincipal,
                        new QueryMetricFactoryImpl());
        List<ResultsPage> pages = new ArrayList<>();
        
        ResultsPage page = runningQuery.next();
        pages.add(page);
        
        // guarantee the need for at least a second page (make the wait slightly longer than the page timeout is set to)
        try {
            Thread.sleep(15);
        } catch (InterruptedException e) {
            
        }
        
        while (page.getStatus() != ResultsPage.Status.COMPLETE) {
            page = runningQuery.next();
            pages.add(page);
        }
        
        // There should be at least 2 pages, more depending on cpu speed.
        assertTrue(pages.size() > 1);
        for (int i = 0; i < pages.size() - 2; ++i) {
            // check every page but the last one for 0 results and PARTIAL status
            assertEquals(0, pages.get(i).getResults().size());
            assertEquals(ResultsPage.Status.PARTIAL, pages.get(i).getStatus());
        }
        // check the last page for COMPLETE status and that the total number of results is 8
        assertEquals(4, pages.get(pages.size() - 2).getResults().size());
        assertEquals(ResultsPage.Status.PARTIAL, pages.get(pages.size() - 2).getStatus());
        assertEquals(4, pages.get(pages.size() - 1).getResults().size());
        assertEquals(ResultsPage.Status.COMPLETE, pages.get(pages.size() - 1).getStatus());
    }
}