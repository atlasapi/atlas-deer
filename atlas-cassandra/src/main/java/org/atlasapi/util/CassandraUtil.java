package org.atlasapi.util;

import com.google.common.base.Function;
import com.netflix.astyanax.connectionpool.OperationResult;


public final class CassandraUtil {

    public static final String KEY = "key";
    public static final String VALUE = "value";

    private CassandraUtil() {}
    
    private enum OperationResultToResultFunction implements Function<OperationResult<Object>, Object> {
        INSTANCE;

        @Override
        public Object apply(OperationResult<Object> opRes) {
            return opRes.getResult();
        }
        
    }
    
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public static <R> Function<OperationResult<R>, R> toResult() {
        return (Function) OperationResultToResultFunction.INSTANCE;
    }
    
}
