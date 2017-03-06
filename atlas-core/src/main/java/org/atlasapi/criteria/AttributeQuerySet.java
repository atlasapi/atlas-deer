package org.atlasapi.criteria;

import java.util.List;

import org.atlasapi.criteria.QueryNode.IntermediateNode;

import com.google.common.collect.ForwardingSet;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

public final class AttributeQuerySet extends ForwardingSet<AttributeQuery<?>> {

    private final IntermediateNode root;
    private final ImmutableSet<AttributeQuery<?>> delegate;

    public AttributeQuerySet(Iterable<? extends AttributeQuery<?>> queries) {
        this.delegate = ImmutableSet.copyOf(queries);
        this.root = new IntermediateNode(ImmutableList.<String>of());
        for (AttributeQuery<?> attributeQuery : queries) {
            ImmutableList<String> path = attributeQuery.getAttribute().getPath();
            root.add(attributeQuery, path, 0);
        }
    }

    @Override
    protected ImmutableSet<AttributeQuery<?>> delegate() {
        return delegate;
    }

    @Override
    public String toString() {
        return root.toString();
    }

    public <V> V accept(QueryNodeVisitor<V> visitor) {
        return root.accept(visitor);
    }

    public <V> List<V> accept(QueryVisitor<V> visitor) {
        ImmutableList.Builder<V> result = ImmutableList.builder();
        for (AttributeQuery<?> query : delegate) {
            V value = query.accept(visitor);
            if (value != null) {
                result.add(value);
            }
        }
        return result.build();
    }

    public AttributeQuerySet copyWith(AttributeQuery<?> query) {
        return new AttributeQuerySet(
                ImmutableList.<AttributeQuery<?>>builder()
                        .addAll(delegate).add(query).build());
    }
}