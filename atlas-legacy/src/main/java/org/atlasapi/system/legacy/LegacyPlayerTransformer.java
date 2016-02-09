package org.atlasapi.system.legacy;

import org.atlasapi.content.Player;
import org.atlasapi.entity.Alias;

import com.google.common.collect.Iterables;

public class LegacyPlayerTransformer
        extends DescribedLegacyResourceTransformer<org.atlasapi.media.entity.Player, Player> {

    @Override
    protected Player createDescribed(org.atlasapi.media.entity.Player input) {
        return new Player();
    }

    @Override
    protected Iterable<Alias> moreAliases(org.atlasapi.media.entity.Player input) {
        return Iterables.transform(
                input.getAliases(),
                alias -> new Alias(alias.getNamespace(), alias.getValue())
        );
    }
}
