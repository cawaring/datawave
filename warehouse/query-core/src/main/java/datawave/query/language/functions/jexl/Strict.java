package datawave.query.language.functions.jexl;

import datawave.query.language.functions.QueryFunction;
import datawave.webservice.query.exception.BadRequestQueryException;
import datawave.webservice.query.exception.DatawaveErrorCode;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;

/**
 * This function accepts a comma separated list of fields to be marked as strict. The purpose is to override lenient model mappings.
 */
public class Strict extends JexlQueryFunction {
    
    public Strict() {
        super("strict", new ArrayList<>());
    }
    
    @Override
    public void validate() throws IllegalArgumentException {
        if (this.parameterList.size() != 1) {
            BadRequestQueryException qe = new BadRequestQueryException(DatawaveErrorCode.INVALID_FUNCTION_ARGUMENTS, MessageFormat.format("{0}", this.name));
            throw new IllegalArgumentException(qe);
        }
    }
    
    @Override
    public QueryFunction duplicate() {
        return new Strict();
    }
    
    @Override
    public String toString() {
        List<String> params = getParameterList();
        return "f:strict(" + String.join("", params) + ")";
    }
}
