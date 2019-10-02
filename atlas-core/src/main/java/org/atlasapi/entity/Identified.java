package org.atlasapi.entity;

import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSortedSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.primitives.Ints;
import org.atlasapi.content.Content;
import org.atlasapi.equivalence.Equivalable;
import org.atlasapi.equivalence.EquivalenceRef;
import org.atlasapi.meta.annotations.FieldName;
import org.joda.time.DateTime;

import javax.annotation.Nullable;
import javax.validation.constraints.NotNull;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Strings.isNullOrEmpty;
import static java.util.Optional.ofNullable;

/**
 * Base type for descriptions of resources.
 */
public class Identified implements Identifiable, Aliased, Sameable {

    private Id id;
    private String canonicalUri;
    private String curie;
    private @Deprecated Set<String> aliasUrls = Sets.newHashSet();
    private ImmutableSet<Alias> aliases = ImmutableSet.of();
    private Set<EquivalenceRef> equivalentTo = Sets.newHashSet();
    private Map<String, String> customFields = Maps.newHashMap();
    /**
     * Records the time that the 3rd party reported that the {@link Identified} was last updated
     */
    private DateTime lastUpdated;
    private DateTime equivalenceUpdate;

    public Identified(String uri, String curie) {
        this.canonicalUri = uri;
        this.curie = curie;
    }

    public Identified() {
        /* allow anonymous entities */
        this.canonicalUri = null;
        this.curie = null;
    }

    public Identified(String uri) {
        this(uri, null);
    }

    public Identified(Id id) {
        this.id = id;
    }

    protected Identified(Builder<?, ?> builder) {
        this.id = builder.id;
        this.canonicalUri = builder.canonicalUri;
        this.aliases = ImmutableSet.copyOf(builder.aliases);
        this.equivalentTo = Sets.newHashSet(builder.equivalentTo);
        this.customFields = Maps.newHashMap(builder.customFields);
        this.lastUpdated = builder.lastUpdated;
        this.equivalenceUpdate = builder.equivalenceUpdate;
        this.curie = builder.curie;
    }

    @FieldName("id")
    @Override
    public Id getId() {
        return id;
    }

    public void setId(long id) {
        this.id = Id.valueOf(id);
    }

    public void setId(Id id) {
        this.id = id;
    }

    @FieldName("uri")
    public String getCanonicalUri() {
        return canonicalUri;
    }

    public void setCanonicalUri(String canonicalUri) {
        this.canonicalUri = canonicalUri;
    }

    @FieldName("curie")
    public String getCurie() {
        return curie;
    }

    public void setCurie(String curie) {
        this.curie = curie;
    }

    @FieldName("alias_urls")
    @Deprecated
    public Set<String> getAliasUrls() {
        return aliasUrls;
    }

    @Deprecated
    public void setAliasUrls(Iterable<String> urls) {
        this.aliasUrls = ImmutableSortedSet.copyOf(urls);
    }

    @FieldName("uris")
    @Deprecated
    public Set<String> getAllUris() {
        Set<String> allUris = Sets.newHashSet(getAliasUrls());
        allUris.add(getCanonicalUri());
        return Collections.unmodifiableSet(allUris);
    }

    @Deprecated
    public void addAliasUrl(String alias) {
        addAliasUrls(ImmutableList.of(alias));
    }

    @Deprecated
    public void addAliasUrls(Iterable<String> urls) {
        setAliasUrls(Iterables.concat(this.aliasUrls, ImmutableList.copyOf(urls)));
    }

    @FieldName("aliases")
    @Override
    public ImmutableSet<Alias> getAliases() {
        return aliases;
    }

    public void setAliases(Iterable<Alias> aliases) {
        this.aliases = ImmutableSet.copyOf(aliases);
    }

    public void addAlias(Alias alias) {
        addAliases(ImmutableList.of(alias));
    }

    public void addAliases(Iterable<Alias> aliases) {
        setAliases(Iterables.concat(this.aliases, ImmutableList.copyOf(aliases)));
    }

    public Set<EquivalenceRef> getEquivalentTo() {
        return equivalentTo;
    }

    public void setEquivalentTo(Set<EquivalenceRef> uris) {
        this.equivalentTo = uris;
    }

    //wut?
    public void addEquivalentTo(Content content) {
        checkNotNull(content.getCanonicalUri());
        this.equivalentTo.add(EquivalenceRef.valueOf(content));
    }

    public Identified copyWithEquivalentTo(Iterable<EquivalenceRef> refs) {
        this.equivalentTo = ImmutableSet.copyOf(refs);
        return this;
    }

    public void setCustomFields(@NotNull Map<String, String> customFields) {
        this.customFields = Maps.newHashMap(checkNotNull(customFields));
    }

    /**
     * Adds a key-value custom field, if the key already exists it will be overwritten
     * Since merging logic will combine all custom fields for everything in the equiv set proper key namespacing
     * may be required to avoid a custom field being ignored in favour of a higher precedence sharing the custom field.
     * @param key the name of the custom field
     * @param value the value of the custom field
     */
    public void addCustomField(@NotNull String key, @Nullable String value) {
        if(value == null) {
            return;
        }
        customFields.put(checkNotNull(key), value);
    }

    /**
     * Adds each key-value entry as a custom field, overwriting existing customFields which share the same key
     * @param customFields the map containing the key-value custom fields to add
     */
    public void addCustomFields(@NotNull Map<String, String> customFields) {
        for(Map.Entry<String, String> entry : customFields.entrySet()) {
            addCustomField(entry.getKey(), entry.getValue());
        }
    }

    @Nullable
    public String getCustomField(@NotNull String key) {
        return customFields.getOrDefault(checkNotNull(key), null);
    }

    public boolean containsCustomFieldKey(@NotNull String key) {
        return customFields.containsKey(key);
    }

    @FieldName("custom_fields")
    public Map<String, String> getCustomFields() {
        return new HashMap<>(customFields);
    }

    public Set<String> getCustomFieldKeys() {
        return getCustomFieldKeys(null);
    }

    public Set<String> getCustomFieldKeys(@Nullable String regex) {
        if(regex == null) {
            return customFields.keySet();
        }
        Pattern regexPattern = Pattern.compile(regex);
        return customFields.keySet()
                .stream()
                .filter(key -> regexPattern.matcher(key).matches())
                .collect(Collectors.toSet());
    }

    @FieldName("last_updated")
    public DateTime getLastUpdated() {
        return lastUpdated;
    }

    public void setLastUpdated(DateTime lastUpdated) {
        this.lastUpdated = lastUpdated;
    }

    @FieldName("equivalence_update")
    public DateTime getEquivalenceUpdate() {
        return equivalenceUpdate;
    }

    public void setEquivalenceUpdate(DateTime equivalenceUpdate) {
        this.equivalenceUpdate = equivalenceUpdate;
    }

    @Override
    public int hashCode() {
        return id != null ? id.hashCode() : super.hashCode();
    }

    @Override
    public boolean equals(Object that) {
        if (this == that) {
            return true;
        }
        if (that instanceof Identified && id != null) {
            Identified other = (Identified) that;
            return id.equals(other.id);
        }
        return false;
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(getClass().getSimpleName().toLowerCase())
                .addValue(id != null ? id : "no-id")
                .toString();
    }

    public static final Comparator<Identified> DESCENDING_LAST_UPDATED = (s1, s2) -> {
        if (s1.getLastUpdated() == null && s2.getLastUpdated() == null) {
            return 0;
        }
        if (s2.getLastUpdated() == null) {
            return -1;
        }
        if (s1.getLastUpdated() == null) {
            return 1;
        }

        return s2.getLastUpdated().compareTo(s1.getLastUpdated());
    };

    /**
     * This method attempts to preserve symmetry of equivalence (since content is persisted
     * independently there is often a window of inconsistency)
     */
    public <T extends Identifiable & Sourced & Equivalable<T>> boolean isEquivalentTo(T content) {
        return getEquivalentTo().contains(EquivalenceRef.valueOf(content))
                || Iterables.contains(Iterables.transform(
                content.getEquivalentTo(),
                Identifiables.toId()
        ), id);
    }

    public static void copyTo(Identified from, Identified to) {
        to.id = from.id;
        to.canonicalUri = from.canonicalUri;
        to.curie = from.curie;
        to.aliasUrls = Sets.newHashSet(from.aliasUrls);
        to.aliases = ImmutableSet.copyOf(from.aliases);
        to.equivalentTo = Sets.newHashSet(from.equivalentTo);
        to.customFields = Maps.newHashMap(from.customFields);
        to.lastUpdated = from.lastUpdated;
        to.equivalenceUpdate = from.equivalenceUpdate;
    }

    /**
     * Same as above except would prefer any value over nulls when copying
     * Needed in the case of barb overrides as they overwrite their equivs
     * data with nulls.
     */
    public static void copyToPreferNonNull(Identified from, Identified to) {
        to.id = ofNullable(from.id).orElse(to.id);
        to.canonicalUri = isNullOrEmpty(from.canonicalUri) ? to.canonicalUri : from.canonicalUri;
        to.curie = isNullOrEmpty(from.curie) ? to.curie : from.curie;
        to.aliasUrls = from.aliasUrls.isEmpty() ? to.aliasUrls : ImmutableSet.copyOf(from.aliasUrls);
        to.aliases = from.aliases.isEmpty() ? to.aliases : ImmutableSet.copyOf(from.aliases);
        to.equivalentTo = from.equivalentTo.isEmpty() ? to.equivalentTo : Sets.newHashSet(from.equivalentTo);
        to.customFields = from.customFields.isEmpty() ? to.customFields : Maps.newHashMap(from.customFields);
        to.lastUpdated = ofNullable(from.lastUpdated).orElse(to.lastUpdated);
        to.equivalenceUpdate = ofNullable(from.equivalenceUpdate).orElse(to.lastUpdated);
    }

    public static <T extends Identified> List<T> sort(List<T> content,
            final Iterable<String> orderIterable) {

        final ImmutableList<String> order = ImmutableList.copyOf(orderIterable);

        Comparator<Identified> byPositionInList = new Comparator<Identified>() {

            @Override
            public int compare(Identified c1, Identified c2) {
                return Ints.compare(indexOf(c1), indexOf(c2));
            }

            private int indexOf(Identified content) {
                for (String uri : content.getAllUris()) {
                    int idx = order.indexOf(uri);
                    if (idx != -1) {
                        return idx;
                    }
                }
                if (content.getCurie() != null) {
                    return order.indexOf(content.getCurie());
                }
                return -1;
            }
        };

        List<T> toSort = Lists.newArrayList(content);
        Collections.sort(toSort, byPositionInList);
        return toSort;
    }

    @Override
    public boolean isSame(@Nullable Sameable other) {
        if (this == other) return true;
        if (other == null || getClass() != other.getClass()) return false;
        Identified that = (Identified) other;
        return java.util.Objects.equals(id, that.id) &&
                java.util.Objects.equals(canonicalUri, that.canonicalUri) &&
                java.util.Objects.equals(curie, that.curie) &&
                java.util.Objects.equals(aliasUrls, that.aliasUrls) &&
                java.util.Objects.equals(aliases, that.aliases) &&
                java.util.Objects.equals(equivalentTo, that.equivalentTo) &&
                java.util.Objects.equals(customFields, that.customFields);
    }

    public static Builder<?, ?> builder() {
        return new IdentifiedBuilder();
    }

    public abstract static class Builder<T extends Identified, B extends Builder<T, B>> {

        private Id id;
        private String canonicalUri;
        private String curie;
        private @Deprecated ImmutableSet<String> aliasUrls = ImmutableSet.of();
        private ImmutableSet<Alias> aliases = ImmutableSet.of();
        private ImmutableSet<EquivalenceRef> equivalentTo = ImmutableSet.of();
        private Map<String, String> customFields = Maps.newHashMap();
        private DateTime lastUpdated;
        private DateTime equivalenceUpdate;

        protected Builder() {
        }

        public B withId(Id id) {
            this.id = id;
            return self();
        }

        public B withCanonicalUri(String canonicalUri) {
            this.canonicalUri = canonicalUri;
            return self();
        }

        public B withCurie(String curie) {
            this.curie = curie;
            return self();
        }

        /**
         * @deprecated Use {@link org.atlasapi.entity.Identified.Builder#withAliases(Iterable)}
         * instead
         */
        @Deprecated
        public B withAliasUrls(Iterable<String> aliasUrls) {
            this.aliasUrls = ImmutableSet.copyOf(aliasUrls);
            return self();
        }

        public B withAliases(Iterable<Alias> aliases) {
            this.aliases = ImmutableSet.copyOf(aliases);
            return self();
        }

        public B withEquivalentTo(Iterable<EquivalenceRef> equivalentTo) {
            this.equivalentTo = ImmutableSet.copyOf(equivalentTo);
            return self();
        }

        public B withCustomFields(Map<String, String> customFields) {
            this.customFields = Maps.newHashMap(customFields);
            return self();
        }

        public B withLastUpdated(DateTime lastUpdated) {
            this.lastUpdated = lastUpdated;
            return self();
        }

        public B withEquivalenceUpdate(DateTime equivalenceUpdate) {
            this.equivalenceUpdate = equivalenceUpdate;
            return self();
        }

        public abstract T build();

        protected abstract B self();
    }

    public static class IdentifiedBuilder extends Builder<Identified, IdentifiedBuilder> {

        private IdentifiedBuilder() {
        }

        @Override
        public Identified build() {
            return new Identified(this);
        }

        @Override
        protected IdentifiedBuilder self() {
            return this;
        }
    }
}
