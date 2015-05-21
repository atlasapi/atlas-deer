package org.atlasapi.system.legacy;


import com.google.common.collect.Iterables;
import org.atlasapi.content.Player;
import org.atlasapi.entity.Alias;

public class LegacyPlayerTransformer extends DescribedLegacyResourceTransformer<org.atlasapi.media.entity.Player, Player>{

    @Override
    protected Player createDescribed(org.atlasapi.media.entity.Player input) {
        return apply(input);
    }

    @Override
    protected Iterable<Alias> moreAliases(org.atlasapi.media.entity.Player input) {
        return Iterables.transform(input.getAliases(), alias -> new Alias(alias.getNamespace(), alias.getValue()));
    }
}
