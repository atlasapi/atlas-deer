package org.atlasapi.util;

import com.google.common.base.Throwables;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionFuture;

import java.util.concurrent.TimeUnit;

public class ElasticsearchUtils {

    public static <T> T getWithTimeout(ActionFuture<T> future, Integer timeoutMillis) {
        try {
            return future.actionGet(timeoutMillis, TimeUnit.MILLISECONDS);
        } catch (ElasticsearchException ese) {
            Throwable root = Throwables.getRootCause(ese);
            Throwables.propagateIfInstanceOf(root, ElasticsearchException.class);
            throw Throwables.propagate(ese);
        }
    }
}
