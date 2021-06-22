package datawave.query.language.functions.jexl;

import datawave.query.jexl.functions.QueryFunctions;
import datawave.query.language.functions.QueryFunction;
import datawave.webservice.query.exception.BadRequestQueryException;
import datawave.webservice.query.exception.DatawaveErrorCode;

import java.text.MessageFormat;
import java.util.ArrayList;

/**
 * Function to return a unique result for every hour of the day for a given list of fields. This function is equivalent to {@code #unique(field[DAY])}.
 */
public class UniqueByHour extends JexlQueryFunction {
    
    public UniqueByHour() {
        super(QueryFunctions.UNIQUE_BY_HOUR_FUNCTION, new ArrayList<>());
    }
    
    /**
     * query options contain a list of fields. Cannot be the empty list.
     * 
     * @throws IllegalArgumentException
     */
    @Override
    public void validate() throws IllegalArgumentException {
        if (this.parameterList.isEmpty()) {
            BadRequestQueryException qe = new BadRequestQueryException(DatawaveErrorCode.INVALID_FUNCTION_ARGUMENTS, MessageFormat.format("{0}", this.name));
            throw new IllegalArgumentException(qe);
        }
    }
    
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        
        sb.append(QueryFunctions.QUERY_FUNCTION_NAMESPACE).append(':').append(this.name);
        if (parameterList.isEmpty()) {
            sb.append("()");
        } else {
            char separator = '(';
            for (String parm : parameterList) {
                sb.append(separator).append(escapeString(parm));
                separator = ',';
            }
            sb.append(')');
        }
        
        return sb.toString();
    }
    
    @Override
    public QueryFunction duplicate() {
        return new UniqueByHour();
    }
}
