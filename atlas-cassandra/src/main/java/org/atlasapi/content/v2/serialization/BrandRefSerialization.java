package org.atlasapi.content.v2.serialization;

import org.atlasapi.content.BrandRef;
import org.atlasapi.content.v2.model.udt.Ref;
import org.atlasapi.entity.Id;
import org.atlasapi.media.entity.Publisher;

public class BrandRefSerialization {

    public Ref serialize(BrandRef brandRef) {
        if (brandRef == null) {
            return null;
        }
        Ref ref = new Ref();

        ref.setId(brandRef.getId().longValue());
        ref.setSource(brandRef.getSource().key());

        return ref;
    }

    public BrandRef deserialize(Ref brandRef) {
        if (brandRef == null) {
            return null;
        }

        return new BrandRef(
                Id.valueOf(brandRef.getId()),
                Publisher.fromKey(brandRef.getSource()).requireValue()
        );
    }

}