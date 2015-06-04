package org.atlasapi.system.legacy;

import org.atlasapi.content.ContentGroup;
import org.atlasapi.content.ContentRef;
import org.atlasapi.entity.Alias;
import org.atlasapi.util.ImmutableCollectors;
import org.atlasapi.util.MorePredicates;

import com.google.common.collect.ImmutableList;

public class LegacyContentGroupTransformer extends DescribedLegacyResourceTransformer<org.atlasapi.media.entity.ContentGroup, ContentGroup> {

    @Override protected ContentGroup createDescribed(org.atlasapi.media.entity.ContentGroup legacyGroup) {

        ContentGroup contentGroup = new ContentGroup(
                legacyGroup.getCanonicalUri(),
                legacyGroup.getPublisher()
        );

        contentGroup.setId(legacyGroup.getId());
        contentGroup.setType(ContentGroup.Type.valueOf(legacyGroup.getType().name()));

        ImmutableList<ContentRef> refs = legacyGroup.getContents().stream()
                .map(ref -> LegacyContentTransformer.legacyRefToRef(ref, legacyGroup.getPublisher()))
                .filter(MorePredicates.isNotNull())
                .collect(ImmutableCollectors.toList());

        contentGroup.setContents(refs);

        return contentGroup;
    }

    @Override protected Iterable<Alias> moreAliases(org.atlasapi.media.entity.ContentGroup input) {
        return ImmutableList.of();
    }
}
