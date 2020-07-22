package org.atlasapi.system.debug;

import java.io.PrintWriter;
import java.io.StringWriter;

import javax.servlet.http.HttpServletResponse;

import org.atlasapi.AtlasPersistenceModule;
import org.atlasapi.content.ContentStore;
import org.atlasapi.content.EquivalentContentStore;
import org.atlasapi.entity.Id;
import org.atlasapi.equivalence.EquivalenceGraphStore;
import org.atlasapi.neo4j.service.Neo4jContentStore;
import org.atlasapi.system.bootstrap.workers.DirectAndExplicitEquivalenceMigrator;
import org.atlasapi.system.legacy.LegacyContentResolver;
import org.atlasapi.system.legacy.LegacySegmentMigrator;

import com.google.common.collect.ImmutableSet;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class ContentDebugControllerTest {

    @Mock private LegacyContentResolver legacyContentResolver;
    @Mock private LegacySegmentMigrator legacySegmentMigrator;
    @Mock private AtlasPersistenceModule atlasPersistenceModule;
    @Mock private DirectAndExplicitEquivalenceMigrator directAndExplicitEquivalenceMigrator;
    @Mock private Neo4jContentStore neo4jContentStore;
    @Mock private ContentStore contentStore;
    @Mock private EquivalenceGraphStore equivalenceGraphStore;
    @Mock private EquivalentContentStore equivalentContentStore;

    @Mock private HttpServletResponse response;

    private ContentDebugController contentDebugController;

    @Before
    public void setUp() throws Exception {
        when(atlasPersistenceModule.nullMessageSendingEquivalenceGraphStore())
                .thenReturn(equivalenceGraphStore);
        when(atlasPersistenceModule.nullMessageSendingEquivalentContentStore())
                .thenReturn(equivalentContentStore);
        when(atlasPersistenceModule.nullMessageSendingContentStore())
                .thenReturn(contentStore);

        contentDebugController = ContentDebugController.builder()
                .withLegacyContentResolver(legacyContentResolver)
                .withLegacySegmentMigrator(legacySegmentMigrator)
                .withPersistence(atlasPersistenceModule)
                .withEquivalenceMigrator(directAndExplicitEquivalenceMigrator)
                .withNeo4jContentStore(neo4jContentStore)
                .withContentStore(contentStore)
                .withContentEquivalenceGraphStore(equivalenceGraphStore)
                .withEquivalentContentStore(equivalentContentStore)
                .build();
    }

    @Test
    public void getContentGraphFromNeo4j() throws Exception {
        StringWriter stringWriter = new StringWriter();
        when(response.getWriter()).thenReturn(new PrintWriter(stringWriter));

        when(neo4jContentStore.getEquivalentSet(Id.valueOf(830L)))
                .thenReturn(ImmutableSet.of(
                        Id.valueOf(1L),
                        Id.valueOf(2L),
                        Id.valueOf(830L)
                ));

        contentDebugController.getContentGraphFromNeo4j("cf2", response);

        assertThat(stringWriter.toString(),
                is("{\"requestedId\":830,\"equivalentSet\":[1,2,830]}"));
    }
}
