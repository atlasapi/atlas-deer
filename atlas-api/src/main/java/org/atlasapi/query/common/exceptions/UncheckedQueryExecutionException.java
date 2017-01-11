package org.atlasapi.query.common.exceptions;

public class UncheckedQueryExecutionException extends RuntimeException {

    public UncheckedQueryExecutionException(QueryExecutionException wrapped) {
        super(wrapped);
    }

}
