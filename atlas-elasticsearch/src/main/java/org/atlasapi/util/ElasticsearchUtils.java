package org.atlasapi.util;

import java.util.concurrent.TimeUnit;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionFuture;

import com.google.common.base.Throwables;
import org.joda.time.DateTime;
import org.joda.time.DateTimeFieldType;

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

    /*
     * This makes the Dates we pass to ElasticSearch cache-useful
     */
    public static DateTime clampDateToFloorMinute(DateTime dt) {
        if (dt == null) {
            return null;
        }
        return dt.withField(DateTimeFieldType.secondOfMinute(), 0)
                .withField(DateTimeFieldType.millisOfSecond(), 0);
    }
}
