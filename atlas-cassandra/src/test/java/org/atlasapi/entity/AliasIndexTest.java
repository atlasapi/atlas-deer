package org.atlasapi.entity;

import org.atlasapi.content.Content;
import org.atlasapi.content.Episode;
import org.atlasapi.media.entity.Publisher;

import com.google.common.collect.ImmutableSet;
import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.connectionpool.exceptions.BadRequestException;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.serializers.StringSerializer;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

public class AliasIndexTest {

    private static final String CF_NAME = "aliases";

    private static final AstyanaxContext<Keyspace> context =
            CassandraHelper.testCassandraContext();

    private static final AliasIndex<Content> index =
            AliasIndex.create(context.getClient(), CF_NAME);

    @BeforeClass
    public static void setup() throws ConnectionException {
        context.start();
        try {
            context.getClient().dropKeyspace();
        } catch (BadRequestException ire) {
        }
        CassandraHelper.createKeyspace(context);
        CassandraHelper.createColumnFamily(context,
                CF_NAME, StringSerializer.get(), StringSerializer.get()
        );
    }

    @AfterClass
    public static void tearDown() throws ConnectionException {
        context.getClient().dropKeyspace();
    }

    @After
    public void clearCf() throws ConnectionException {
        CassandraHelper.clearColumnFamily(context, CF_NAME);
    }

    @Test
    public void testMutatingAliases() throws Exception {

        Alias alias1 = new Alias("namespace1", "value1");
        Alias alias2 = new Alias("namespace2", "value2");

        Episode ep = new Episode();
        ep.setId(1234);
        ep.setPublisher(Publisher.BBC);
        ep.setAliases(ImmutableSet.of(alias1));

        //initial indexing
        index.mutateAliases(ep, null).execute();

        assertThat(index.readAliases(Publisher.BBC, ImmutableSet.of(alias1)).isEmpty(), is(false));

        Episode prev = new Episode();
        prev.setId(1234);
        prev.setPublisher(Publisher.BBC);
        prev.setAliases(ImmutableSet.of(alias1));

        ep.setAliases(ImmutableSet.of(alias1, alias2));

        // add second alias
        index.mutateAliases(ep, prev).execute();

        assertThat(index.readAliases(Publisher.BBC, ImmutableSet.of(alias1)).isEmpty(), is(false));
        assertThat(index.readAliases(Publisher.BBC, ImmutableSet.of(alias2)).isEmpty(), is(false));

        prev.setAliases(ep.getAliases());
        ep.setAliases(ImmutableSet.of(alias2));

        //remove first alias
        index.mutateAliases(ep, prev).execute();

        assertThat(index.readAliases(Publisher.BBC, ImmutableSet.of(alias1)).isEmpty(), is(true));
        assertThat(index.readAliases(Publisher.BBC, ImmutableSet.of(alias2)).isEmpty(), is(false));

        prev.setAliases(ep.getAliases());
        Alias alias3 = new Alias("namespace3", "value3");
        ep.setAliases(ImmutableSet.of(alias3));

        //switch second alias for third
        index.mutateAliases(ep, prev).execute();

        assertThat(index.readAliases(Publisher.BBC, ImmutableSet.of(alias1)).isEmpty(), is(true));
        assertThat(index.readAliases(Publisher.BBC, ImmutableSet.of(alias2)).isEmpty(), is(true));
        assertThat(index.readAliases(Publisher.BBC, ImmutableSet.of(alias3)).isEmpty(), is(false));
    }

}
