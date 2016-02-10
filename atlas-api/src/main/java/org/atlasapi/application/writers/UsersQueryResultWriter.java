package org.atlasapi.application.writers;

import java.io.IOException;

import org.atlasapi.application.users.User;
import org.atlasapi.output.EntityListWriter;
import org.atlasapi.output.OutputContext;
import org.atlasapi.output.ResponseWriter;
import org.atlasapi.output.useraware.UserAwareQueryResult;
import org.atlasapi.output.useraware.UserAwareQueryResultWriter;
import org.atlasapi.query.common.useraware.UserAwareQueryContext;

import com.google.common.collect.FluentIterable;

public class UsersQueryResultWriter implements UserAwareQueryResultWriter<User> {

    private final EntityListWriter<User> usersListWriter;

    public UsersQueryResultWriter(EntityListWriter<User> usersListWriter) {
        this.usersListWriter = usersListWriter;
    }

    @Override
    public void write(UserAwareQueryResult<User> result, ResponseWriter responseWriter)
            throws IOException {
        responseWriter.startResponse();
        writeResult(result, responseWriter);
        responseWriter.finishResponse();
    }

    private void writeResult(UserAwareQueryResult<User> result, ResponseWriter writer)
            throws IOException {
        OutputContext ctxt = outputContext(result.getContext());

        if (result.isListResult()) {
            FluentIterable<User> resources = result.getResources();
            writer.writeList(usersListWriter, resources, ctxt);
        } else {
            writer.writeObject(usersListWriter, result.getOnlyResource(), ctxt);
        }
    }

    private OutputContext outputContext(UserAwareQueryContext queryContext) {
        return OutputContext.valueOf(queryContext);
    }

}
