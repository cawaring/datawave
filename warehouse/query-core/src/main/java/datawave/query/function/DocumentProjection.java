package datawave.query.function;

import com.google.common.collect.Maps;
import datawave.query.attributes.Attribute;
import datawave.query.attributes.Document;
import datawave.query.predicate.Projection;
import org.apache.accumulo.core.data.Key;
import org.apache.log4j.Logger;

import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

/**
 * Applies an includes or excludes projection to a Document. Includes projection will preserve Document sub-substructure whereas excludes projection will prune
 * sub-substructure which does not match the excludes.
 * <p>
 * e.g. Input: {NAME:'bob', CHILDREN:[{NAME:'frank', AGE:12}, {NAME:'sally', AGE:10}], AGE:40}
 * <p>
 * Include of 'NAME' applied: {NAME:'bob', CHILDREN:[{NAME:'frank'}, {NAME:'sally'}]}
 * <p>
 * Exclude of 'NAME' applied: {CHILDREN:[{AGE:12}, {AGE:10}], AGE:40}
 */
public class DocumentProjection implements DocumentPermutation {
    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(DocumentProjection.class);
    
    private final boolean includeGroupingContext;
    private final boolean reducedResponse;
    private final Projection projection;
    
    /**
     * should track document sizes
     */
    private boolean trackSizes = true;
    
    public DocumentProjection() {
        this(false, false);
    }
    
    public DocumentProjection(boolean includeGroupingContext, boolean reducedResponse) {
        this(includeGroupingContext, reducedResponse, true);
    }
    
    public DocumentProjection(boolean includeGroupingContext, boolean reducedResponse, boolean trackSizes) {
        this.includeGroupingContext = includeGroupingContext;
        this.reducedResponse = reducedResponse;
        this.projection = new Projection();
        this.trackSizes = trackSizes;
    }
    
    /**
     * Set the delegate {@link Projection} with the fields to include
     *
     * @param includes
     *            the set of fields to include
     */
    public void setIncludes(Set<String> includes) {
        this.projection.setIncludes(includes);
    }
    
    /**
     * Configure the delegate {@link Projection} with the fields to exclude
     *
     * @param excludes
     *            the set of fields to exclude
     */
    public void setExcludes(Set<String> excludes) {
        this.projection.setExcludes(excludes);
    }
    
    public Projection getProjection() {
        return projection;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.google.common.base.Function#apply(java.lang.Object)
     */
    @Override
    public Entry<Key,Document> apply(Entry<Key,Document> from) {
        Document returnDoc = trim(from.getValue());
        return Maps.immutableEntry(from.getKey(), returnDoc);
    }
    
    private Document trim(Document d) {
        if (log.isTraceEnabled()) {
            log.trace("Applying projection " + projection + " to " + d);
        }
        Map<String,Attribute<? extends Comparable<?>>> dict = d.getDictionary();
        Document newDoc = new Document();
        
        for (Entry<String,Attribute<? extends Comparable<?>>> entry : dict.entrySet()) {
            String fieldName = entry.getKey();
            Attribute<?> attr = entry.getValue();
            
            if (projection.apply(fieldName)) {
                
                // We just want to add this subtree
                newDoc.put(fieldName, (Attribute<?>) attr.copy(), this.includeGroupingContext, this.reducedResponse);
                
            }
        }
        
        if (log.isTraceEnabled()) {
            log.trace("Document after projection: " + newDoc);
        }
        
        return newDoc;
    }
    
}
