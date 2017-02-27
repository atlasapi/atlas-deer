package org.atlasapi.criteria;

import org.atlasapi.criteria.QueryNode.IntermediateNode;

import com.google.common.collect.ForwardingSet;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

public final class AttributeQuerySet extends ForwardingSet<AttributeQuery<?>> {

    // We need both the tree root and the ForwardingSet delegate because:
    // - some code accesses this by visiting/traversing the tree
    // - some code accesses this by using it as an iterable and iterating over delegate
    private final IntermediateNode root;
    private final ImmutableSet<AttributeQuery<?>> delegate;

    private AttributeQuerySet(Iterable<? extends AttributeQuery<?>> queries) {
        this.delegate = ImmutableSet.copyOf(queries);
        this.root = new IntermediateNode(ImmutableList.of());

        for (AttributeQuery<?> attributeQuery : queries) {
            ImmutableList<String> path = attributeQuery.getAttribute().getPath();
            root.add(attributeQuery, path, 0);
        }
    }

    @Override
    protected ImmutableSet<AttributeQuery<?>> delegate() {
        return delegate;
    }

    public static AttributeQuerySet create(Iterable<? extends AttributeQuery<?>> queries) {
        return new AttributeQuerySet(queries);
    }

    public <V> V accept(QueryNodeVisitor<V> visitor) {
        return root.accept(visitor);
    }

    public AttributeQuerySet copyWith(AttributeQuery<?> query) {
        return create(
                ImmutableList.<AttributeQuery<?>>builder()
                        .addAll(delegate)
                        .add(query)
                        .build()
        );
    }

    @Override
    public String toString() {
        return root.toString();
    }
}
