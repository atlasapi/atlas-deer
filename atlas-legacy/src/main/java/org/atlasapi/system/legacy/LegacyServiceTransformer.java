package org.atlasapi.system.legacy;

import org.atlasapi.content.Service;
import org.atlasapi.entity.Alias;

import com.google.common.collect.Iterables;

public class LegacyServiceTransformer
        extends DescribedLegacyResourceTransformer<org.atlasapi.media.entity.Service, Service> {

    @Override
    protected Service createDescribed(org.atlasapi.media.entity.Service input) {
        return new Service();
    }

    @Override
    protected Iterable<Alias> moreAliases(org.atlasapi.media.entity.Service input) {
        return Iterables.transform(
                input.getAliases(),
                alias -> new Alias(alias.getNamespace(), alias.getValue())
        );
    }
}