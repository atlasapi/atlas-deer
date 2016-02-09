package org.atlasapi.application.writers;

import java.io.IOException;

import org.atlasapi.application.sources.SourceIdCodec;
import org.atlasapi.application.users.User;
import org.atlasapi.entity.Id;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.output.EntityListWriter;
import org.atlasapi.output.EntityWriter;
import org.atlasapi.output.FieldWriter;
import org.atlasapi.output.OutputContext;
import org.atlasapi.query.common.Resource;

import com.metabroadcast.common.ids.NumberToShortStringCodec;
import com.metabroadcast.common.social.model.UserRef;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;

public class UsersListWriter implements EntityListWriter<User> {

    private final EntityWriter<UserRef> userRefWriter = UserRefWriter.userRefWriter("userRef");
    private final EntityListWriter<Publisher> sourcesWriter;
    private final NumberToShortStringCodec idCodec;

    private final Function<Id, String> ENCODE_APP_IDS = new Function<Id, String>() {

        @Override
        public String apply(Id input) {
            return idCodec.encode(input.toBigInteger());
        }

    };

    public UsersListWriter(NumberToShortStringCodec idCodec,
            SourceIdCodec sourceIdCodec) {
        this.idCodec = idCodec;
        this.sourcesWriter = new SourceWithIdWriter(sourceIdCodec, "sources", "sources");
    }

    private Iterable<String> getStringAppIds(User user) {
        return Iterables.transform(user.getApplicationIds(), ENCODE_APP_IDS);
    }

    @Override
    public void write(User entity, FieldWriter writer, OutputContext ctxt) throws IOException {
        ctxt.startResource(Resource.APPLICATION);
        writer.writeField("id", idCodec.encode(entity.getId().toBigInteger()));
        writer.writeObject(userRefWriter, entity.getUserRef(), ctxt);
        writer.writeField("screen_name", entity.getScreenName());
        writer.writeField("full_name", entity.getFullName());
        writer.writeField("company", entity.getCompany());
        writer.writeField("email", entity.getEmail());
        writer.writeField("website", entity.getWebsite());
        writer.writeField("profile_image", entity.getProfileImage());
        writer.writeList("applications", "application", getStringAppIds(entity), ctxt);
        writer.writeList(sourcesWriter, entity.getSources(), ctxt);
        writer.writeField("role", entity.getRole().toString().toLowerCase());
        writer.writeField("profile_complete", entity.isProfileComplete());
        writer.writeField("license_accepted", entity.getLicenseAccepted().orNull());
        ctxt.endResource();
    }

    @Override
    public String fieldName(User entity) {
        return Resource.USER.getSingular();
    }

    @Override
    public String listName() {
        return Resource.USER.getPlural();
    }

}
