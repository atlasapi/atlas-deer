package org.atlasapi.application.users;

import org.atlasapi.entity.Id;

import com.metabroadcast.common.ids.IdGenerator;
import com.metabroadcast.common.ids.SubstitutionTableNumberCodec;

import com.google.common.base.Supplier;

import static com.google.common.base.Preconditions.checkNotNull;

public class NewUserSupplier implements Supplier<User> {

    private final IdGenerator idGenerator;
    private SubstitutionTableNumberCodec codec;

    public NewUserSupplier(IdGenerator idGenerator) {
        this.idGenerator = checkNotNull(idGenerator);
        this.codec = new SubstitutionTableNumberCodec();
    }

    @Override
    public User get() {
        return User.builder().withId(Id.valueOf(codec.decode(idGenerator.generate()))).build();
    }
}
