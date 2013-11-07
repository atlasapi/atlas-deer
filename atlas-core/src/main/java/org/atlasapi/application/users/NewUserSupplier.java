package org.atlasapi.application.users;

import org.atlasapi.entity.Id;

import com.google.common.base.Supplier;
import com.metabroadcast.common.ids.IdGenerator;
import com.metabroadcast.common.ids.SubstitutionTableNumberCodec;

public class NewUserSupplier implements Supplier<User> {

    private final IdGenerator idGenerator;
    private SubstitutionTableNumberCodec codec;

    public NewUserSupplier(IdGenerator idGenerator) {
        this.idGenerator = idGenerator;
        this.codec = new SubstitutionTableNumberCodec();
    }
    
    @Override
    public User get() {
        return User.builder().withId(Id.valueOf(codec.decode(idGenerator.generate()))).build();
    }

}
