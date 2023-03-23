package datawave.query.iterator.filter;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.Map;
import java.util.Set;

import datawave.edge.model.EdgeModelAware;
import datawave.edge.model.EdgeModelAware.Fields;
import datawave.edge.model.EdgeModelAware.Fields.FieldKey;
import datawave.edge.util.EdgeKeyUtil;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.Filter;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.jexl2.Expression;
import org.apache.commons.jexl2.JexlContext;
import org.apache.commons.jexl2.JexlEngine;
import org.apache.commons.jexl2.MapContext;
import org.apache.log4j.Logger;

import com.google.common.collect.HashMultimap;

/**
 * This is a simple JEXL query filter iterator used in conjunction with the EdgeQueryLogic to evaluate more complicated expressions against edge keys.
 *
 * EdgeQuery by itself will configure ranges, and regex filters that will return ALL keys that could satisfy the supplied JEXL query. This filter is intended to
 * pair the returned values from that iterator stack down to only those that actually do adhere to the full JEXL expression.
 *
 * Prefiltering is an optional component that can determine quickly if a key will fail using a whitelist of accepted values parsed from the jexl
 *
 */
public class EdgeFilterIterator extends Filter {
    public static final Logger log = Logger.getLogger(EdgeFilterIterator.class);
    
    public static final String JEXL_OPTION = "jexlQuery";
    public static final String PROTOBUF_OPTION = "protobuffFormat";
    public static final String INCLUDE_STATS_OPTION = "includeStats";
    public static final String JEXL_STATS_OPTION = "jexlStatsQuery";
    public static final String PREFILTER_WHITELIST = "prefilter";
    
    private static final JexlEngine jexlEngine = new JexlEngine();
    
    private boolean protobuffFormat;
    private boolean includeStatsEdges;
    private Expression expression = null;
    private Expression statsExpression = null;
    private JexlContext ctx = new MapContext();
    
    private HashMultimap<String,String> preFilterValues;
    
    @Override
    public SortedKeyValueIterator<Key,Value> deepCopy(IteratorEnvironment env) {
        EdgeFilterIterator result = (EdgeFilterIterator) super.deepCopy(env);
        result.protobuffFormat = this.protobuffFormat;
        result.expression = this.expression;
        result.preFilterValues = this.preFilterValues;
        
        return result;
    }
    
    @Override
    public IteratorOptions describeOptions() {
        IteratorOptions io = super.describeOptions();
        
        io.addNamedOption(JEXL_OPTION, "The JEXL query string to evaulate edges against.");
        io.setName("edgeJexlFilter");
        io.setDescription("EdgeFilterIterator evaluates arbitrary JEXL expressions against edge table keys.");
        
        io.addNamedOption(PROTOBUF_OPTION, "Use protocol buffer edge table format?");
        io.setDescription("Determines which edge table format this iterator is supposed to be iterating over.");
        
        io.addNamedOption(INCLUDE_STATS_OPTION, "Include stats edges in the query result?");
        io.setDescription("Will automatically return any stats edges");
        
        io.addNamedOption(JEXL_STATS_OPTION, "Query string to evaluate stats edges against");
        io.setDescription("Evalueates arbitrary JEXL expressions agains stats edge table keys");
        
        io.addNamedOption(PREFILTER_WHITELIST, "Serialized Hashmultimap of fieldname:fieldvalue for prefiltering.");
        io.setDescription("Used to filter keys prior to building a jexl context.");
        return io;
    }
    
    /**
     * Sets up the jexl context with the terms to evaluate against.
     *
     * Converts them all to lowercase for case-insensitive queries.
     *
     *
     *
     * @param ctx
     *            the context
     * @param keyComponents
     *            mapping of key components
     */
    private void setupContext(JexlContext ctx, Map<FieldKey,String> keyComponents) {
        
        String source = keyComponents.get(FieldKey.EDGE_SOURCE);
        String sink = keyComponents.get(FieldKey.EDGE_SINK);
        String edgeType = keyComponents.get(FieldKey.EDGE_TYPE);
        String edgeRelationship = keyComponents.get(FieldKey.EDGE_RELATIONSHIP);
        String edgeAttribute1 = keyComponents.get(FieldKey.EDGE_ATTRIBUTE1);
        String edgeAttribute2 = keyComponents.get(FieldKey.EDGE_ATTRIBUTE2);
        String edgeAttribute3 = keyComponents.get(FieldKey.EDGE_ATTRIBUTE3);
        String edgeDate = keyComponents.get(FieldKey.DATE);
        
        // Convert all values to lowercase.
        if (null != source)
            source = source.toLowerCase();
        if (null != sink)
            sink = sink.toLowerCase();
        if (null != edgeType)
            edgeType = edgeType.toLowerCase();
        if (null != edgeRelationship)
            edgeRelationship = edgeRelationship.toLowerCase();
        if (null != edgeAttribute1)
            edgeAttribute1 = edgeAttribute1.toLowerCase();
        if (null != edgeAttribute2)
            edgeAttribute2 = edgeAttribute2.toLowerCase();
        if (null != edgeAttribute3)
            edgeAttribute3 = edgeAttribute3.toLowerCase();
        if (null != edgeDate)
            edgeDate = edgeDate.toLowerCase();
        
        ctx.set(EdgeModelAware.EDGE_SOURCE.toLowerCase(), source);
        ctx.set(EdgeModelAware.EDGE_SINK.toLowerCase(), sink);
        ctx.set(EdgeModelAware.EDGE_TYPE.toLowerCase(), edgeType);
        ctx.set(EdgeModelAware.EDGE_RELATIONSHIP.toLowerCase(), edgeRelationship);
        ctx.set(EdgeModelAware.EDGE_ATTRIBUTE1.toLowerCase(), edgeAttribute1);
        ctx.set(EdgeModelAware.EDGE_ATTRIBUTE2.toLowerCase(), edgeAttribute2);
        ctx.set(EdgeModelAware.EDGE_ATTRIBUTE3.toLowerCase(), edgeAttribute3);
        ctx.set(EdgeModelAware.DATE.toLowerCase(), edgeDate);
    }
    
    /**
     * Method to setup the jexl query expression from the iterator options for evaulation.
     *
     * @param options
     *            mapping of options
     */
    private void initOptions(Map<String,String> options) {
        String jexl = options.get(JEXL_OPTION);
        if (null == jexl) {
            throw new IllegalArgumentException("Edge filter not configured with query string! Please configure parameter: " + JEXL_OPTION
                            + " with the JEXL query.");
        }
        // to stay consistent with the rest of the query engine, support case-insensitive boolean operators.
        String caseFixQuery = jexl.toLowerCase();
        expression = jexlEngine.createExpression(caseFixQuery);
        
        String protobuff = options.get(PROTOBUF_OPTION);
        if (null == protobuff) {
            throw new IllegalArgumentException("Must specify if this is a protocol buffer edge formatted table.");
        }
        protobuffFormat = Boolean.parseBoolean(protobuff);
        
        String incldStats = options.get(INCLUDE_STATS_OPTION);
        if (null == incldStats) {
            throw new IllegalArgumentException("Must specify if stats edges should be returned.");
        }
        includeStatsEdges = Boolean.parseBoolean(incldStats);
        
        String jexlStats = options.get(JEXL_STATS_OPTION);
        
        if (jexlStats != null) {
            statsExpression = jexlEngine.createExpression(jexlStats.toLowerCase());
        }
        
        String inPrefilter = options.get(PREFILTER_WHITELIST);
        
        if (null != inPrefilter) {
            byte[] data = Base64.decodeBase64(inPrefilter);
            ObjectInputStream ois = null;
            try {
                ois = new ObjectInputStream(new ByteArrayInputStream(data));
                Object o = ois.readObject();
                preFilterValues = (HashMultimap<String,String>) o;
            } catch (IOException ex) {
                // we can work without it
                log.error("Invalid whitelist value supplied to iterator.");
            } catch (ClassNotFoundException ex) {
                log.error("Class not found for whitelies value.");
            }
        }
    }
    
    /**
     * Method to perform prefilter against a whitelist to see if we can quickly ignore the key
     *
     * @param keyComponents
     *            mapping of key components
     * @return if we can ignore the key
     */
    private boolean prefilter(Map<FieldKey,String> keyComponents) {
        boolean retVal = true;
        if (preFilterValues != null) {
            for (Map.Entry<FieldKey,String> entry : keyComponents.entrySet()) {
                String fieldName = Fields.getInstance().getFieldName(entry.getKey());
                Set<String> values = preFilterValues.get(fieldName);
                if (values == null || values.size() < 1) {
                    // if we encountered a regex, we'll just let the jexl engine handle it, or filter it by a different field
                    continue;
                }
                // assuming we have no regex to match on this field, then if the whitelist exists
                // and the value for this key isn't in that whitelist, we just give it the boot.
                if (!preFilterValues.get(fieldName).contains(entry.getValue())) {
                    return false;
                }
            }
        }
        
        return retVal;
    }
    
    @Override
    public void init(org.apache.accumulo.core.iterators.SortedKeyValueIterator<org.apache.accumulo.core.data.Key,org.apache.accumulo.core.data.Value> source,
                    java.util.Map<java.lang.String,java.lang.String> options, org.apache.accumulo.core.iterators.IteratorEnvironment env)
                    throws java.io.IOException {
        super.init(source, options, env);
        initOptions(options);
    }
    
    /**
     * For testing purposes only. Does nothing with super.
     *
     * @param source
     *            a source
     * @param options
     *            map of options
     * @throws java.io.IOException
     *             for issues with read/write
     */
    public void init(org.apache.accumulo.core.iterators.SortedKeyValueIterator<org.apache.accumulo.core.data.Key,org.apache.accumulo.core.data.Value> source,
                    java.util.Map<java.lang.String,java.lang.String> options) throws java.io.IOException {
        initOptions(options);
    }
    
    /**
     * Determines if the edge key satisfies the conditions expressed in the supplied JEXL query string.
     *
     * @param k
     *            a key
     * @param V
     *            a value
     * @return boolean - true if it is a match.
     */
    @Override
    public boolean accept(Key k, Value V) {
        boolean value = false;
        
        Map<FieldKey,String> keyComponents = EdgeKeyUtil.dissasembleKey(k, protobuffFormat);
        
        if (!prefilter(keyComponents)) {
            value = false;
        } else if (keyComponents.containsKey(FieldKey.STATS_EDGE)) {
            if (includeStatsEdges) {
                if (statsExpression != null) {
                    setupContext(ctx, keyComponents);
                    value = (boolean) statsExpression.evaluate(ctx);
                } else {
                    value = true;
                }
            } else {
                value = false;
            }
        } else {
            setupContext(ctx, keyComponents);
            value = (boolean) expression.evaluate(ctx);
        }
        
        return value;
    }
}
