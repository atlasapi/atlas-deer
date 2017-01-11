package org.atlasapi.query.common.attributes;

import org.atlasapi.criteria.attribute.Attribute;

import com.google.common.base.Optional;

public final class AttributeLookupTree extends PrefixInTree<Attribute<?>> {

    private AttributeLookupTree() {
    }

    public static AttributeLookupTree create() {
        return new AttributeLookupTree();
    }

    public Optional<Attribute<?>> attributeFor(String key) {
        return super.valueForKeyPrefixOf(key);
    }

    public void put(Attribute<?> attribute) {
        put(attribute.externalName(), Optional.<Attribute<?>>of(attribute));
    }
}
