package org.atlasapi.application.writers;

import java.io.IOException;

import org.atlasapi.output.EntityListWriter;
import org.atlasapi.output.EntityWriter;
import org.atlasapi.output.FieldWriter;
import org.atlasapi.output.OutputContext;

import com.metabroadcast.common.social.model.UserRef;

import static com.google.common.base.Preconditions.checkNotNull;

public class UserRefWriter implements EntityListWriter<UserRef> {

    public static final EntityListWriter<UserRef> userRefListWriter(String listName) {
        return new UserRefWriter(checkNotNull(listName), "source");
    }

    public static final EntityWriter<UserRef> userRefWriter(String fieldName) {
        return new UserRefWriter(null, checkNotNull(fieldName));
    }

    private final String listName;
    private final String fieldName;

    private UserRefWriter(String listName, String fieldName) {
        this.listName = listName;
        this.fieldName = fieldName;
    }

    @Override
    public void write(UserRef entity, FieldWriter writer, OutputContext ctxt) throws IOException {
        writer.writeField("userId", entity.getUserId());
        writer.writeField("userNamespace", entity.getNamespace().name().toLowerCase());
        writer.writeField("appId", entity.getAppId());
        writer.writeField("opaqueId", entity.getOpaqueId());
    }

    @Override
    public String listName() {
        return listName;
    }

    @Override
    public String fieldName(UserRef entity) {
        return fieldName;
    }
}
