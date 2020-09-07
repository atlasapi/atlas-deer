package org.atlasapi.criteria.attribute;

import org.atlasapi.entity.Identified;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;

import static com.google.common.base.Preconditions.checkNotNull;

public abstract class Attribute<T> implements QueryFactory<T> {

    private static final Splitter PATH_SPLITTER = Splitter.on('.').omitEmptyStrings().trimResults();
    private static final Joiner PATH_JOINER = Joiner.on('.');

    protected final String name;
    private final String pathPrefix;
    private final ImmutableList<String> pathParts;

    private String javaAttributeName;
    private final Class<? extends Identified> target;
    private String alias;

    // isCollectionOfValues is never used, perhaps there were plans for a refactor that were lost to
    // the ether, this means that for now, every parameter is treated as a collection of values.
    // We are keeping the field as is since it may be used in the future.
    private final boolean isCollectionOfValues;
    private final boolean shouldSplitValuesIntoList; // setting this to false explicitly states that
    // we want the attribute to be treated as a single valued parameter.

    protected Attribute(
            String name,
            Class<? extends Identified> target,
            boolean isCollectionOfValues
    ) {
        this(name, name, target, isCollectionOfValues);
    }

    protected Attribute(
            String name,
            Class<? extends Identified> target,
            boolean isCollectionOfValues,
            boolean shouldSplitValuesIntoList
    ) {
        this(name, name, target, isCollectionOfValues, shouldSplitValuesIntoList);
    }

    protected Attribute(
            String name,
            String javaAttributeName,
            Class<? extends Identified> target,
            boolean isCollectionOfValues
    ) {
        this(name, javaAttributeName, target, isCollectionOfValues, true);
    }

    protected Attribute(
            String name,
            String javaAttributeName,
            Class<? extends Identified> target,
            boolean isCollectionOfValues,
            boolean shouldSplitValuesIntoList
    ) {
        this.name = checkNotNull(name);
        this.javaAttributeName = checkNotNull(javaAttributeName);
        this.pathParts = ImmutableList.copyOf(PATH_SPLITTER.split(javaAttributeName));
        this.pathPrefix = PATH_JOINER.join(pathParts.subList(0, pathParts.size() - 1));
        this.target = target;
        this.isCollectionOfValues = isCollectionOfValues;
        this.shouldSplitValuesIntoList = shouldSplitValuesIntoList;
    }

    @Override
    public int hashCode() {
        return name.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof Attribute<?>) {
            Attribute<?> attribute = (Attribute<?>) obj;
            return name.equals(attribute.name) && target.equals(attribute.target);
        }
        return false;
    }

    public String externalName() {
        return name;
    }

    public String javaAttributeName() {
        return javaAttributeName;
    }

    public Class<? extends Identified> target() {
        return target;
    }

    public boolean isCollectionOfValues() {
        return isCollectionOfValues;
    }

    public boolean shouldSplitValuesIntoList() {
        return shouldSplitValuesIntoList;
    }

    public Attribute<T> allowShortMatches() {
        this.alias = name;
        return this;
    }

    public Attribute<T> withAlias(String alias) {
        this.alias = alias;
        return this;
    }

    public String alias() {
        return alias;
    }

    public boolean hasAlias() {
        return alias != null;
    }

    public ImmutableList<String> getPath() {
        return pathParts;
    }

    public String getPathPrefix() {
        return pathPrefix;
    }
}
