package org.atlasapi.query.common;

public class UncheckedQueryExecutionException extends RuntimeException {

    public UncheckedQueryExecutionException(QueryExecutionException wrapped) {
        super(wrapped);
    }

}
