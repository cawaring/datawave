package datawave.query.function;

import java.util.HashSet;
import java.util.Set;

public class MatchingFieldGroup {
    private final Set<String> matchingFieldSet;
    private final Set<String> matchingValues;
    
    public MatchingFieldGroup(Set<String> matchingFieldSet) {
        this.matchingFieldSet = matchingFieldSet;
        this.matchingValues = new HashSet<>();
    }
    
    public boolean containsField(String fieldNoGrouping) {
        return matchingFieldSet.contains(fieldNoGrouping);
    }
    
    public void addMatchingValue(String value) {
        matchingValues.add(value);
    }
    
    public boolean containsValue(String value) {
        return matchingValues.contains(value);
    }
}
