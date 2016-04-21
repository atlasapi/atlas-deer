package org.atlasapi.output.useraware;

import java.io.IOException;

import org.atlasapi.output.ResponseWriter;

public interface UserAccountsAwareQueryResultWriter<T> {

    void write(UserAccountsAwareQueryResult<T> result, ResponseWriter responseWriter) throws IOException;

}
