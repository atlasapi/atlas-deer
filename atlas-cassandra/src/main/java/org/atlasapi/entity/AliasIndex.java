package org.atlasapi.entity;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Collection;
import java.util.List;
import java.util.Set;

import javax.annotation.Nullable;

import com.google.common.base.Throwables;
import org.atlasapi.media.entity.Publisher;

import com.google.common.base.Function;
import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.model.Column;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.Row;
import com.netflix.astyanax.model.Rows;
import com.netflix.astyanax.query.RowSliceQuery;
import com.netflix.astyanax.serializers.StringSerializer;

public final class AliasIndex<A extends Identifiable & Sourced & Aliased> {

    public static final class AliasSerializer implements Function<Alias, String> {
        @Override
        public String apply(Alias input) {
            return serialize(input);
        }
    }

    public static final <A extends Identifiable & Aliased & Sourced> AliasIndex<A> create(Keyspace keyspace, String name) {
        return new AliasIndex<A>(keyspace, ColumnFamily.newColumnFamily(name,
                StringSerializer.get(), StringSerializer.get()));
    }

    private final Keyspace keyspace;
    private final ColumnFamily<String, String> columnFamily;
    
    private final AliasSerializer toSerializedForm;

    private AliasIndex(Keyspace keyspace, ColumnFamily<String, String> columnFamily) {
        this.keyspace = checkNotNull(keyspace);
        this.columnFamily = checkNotNull(columnFamily);
        this.toSerializedForm = new AliasSerializer();
    }
    
    private static final String serialize(Alias alias) {
        return alias.getNamespace() + ":" + alias.getValue();
    }
    
    private Collection<String> serialize(Collection<Alias> aliases) {
        return Collections2.transform(aliases, toSerializedForm);
    }
    
    public MutationBatch mutateAliases(A resource, @Nullable A previous) {
        checkNotNull(resource);
        String columnKey = resource.getSource().key();
        long resourceId = resource.getId().longValue();

        MutationBatch batch = keyspace.prepareMutationBatch();
        Collection<Alias> newAliases = newAliases(resource, previous);
        for(String serializedAlias : serialize(newAliases)) {
            batch.withRow(columnFamily, serializedAlias)
                .putColumn(columnKey, resourceId);
        }
        if (previous != null) {
            Collection<Alias> oldAliases = 
                    Sets.difference(previous.getAliases(), resource.getAliases());
            for (String serializedAlias : serialize(oldAliases)) {
                batch.withRow(columnFamily, serializedAlias)
                    .deleteColumn(columnKey);
            }
        }
        return batch;
    }

    public void mutateAliasesAndExecute(A resource, @Nullable A previous) {
        try {
            mutateAliases(resource, previous).execute();
        } catch (ConnectionException e) {
            throw Throwables.propagate(e);
        }
    }

    private Set<Alias> newAliases(A resource, A previous) {
        return previous != null ? Sets.difference(resource.getAliases(), previous.getAliases())
                                : resource.getAliases();
    }
    
    public Set<Long> readAliases(Publisher source, Iterable<Alias> aliases) throws ConnectionException {
        ImmutableSet<Alias> uniqueAliases = ImmutableSet.copyOf(aliases);
        String columnName = checkNotNull(source).key();
        RowSliceQuery<String, String> aliasQuery = keyspace.prepareQuery(columnFamily)
            .getRowSlice(serialize(uniqueAliases.asList()))
            .withColumnSlice(columnName);
        Rows<String,String> rows = aliasQuery.execute().getResult();
        
        List<Long> ids = Lists.newArrayListWithCapacity(rows.size());
        for (Row<String, String> row : rows) {
            Column<String> idCell = row.getColumns().getColumnByName(columnName);
            if (idCell != null) {
                ids.add(idCell.getLongValue());
            }
        }
        return ImmutableSet.copyOf(ids);
    }

    
}
