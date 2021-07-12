package org.atlasapi.system.bootstrap.workers;

import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Futures;
import com.metabroadcast.common.time.Timestamp;
import org.atlasapi.entity.Id;
import org.atlasapi.entity.ResourceRef;
import org.atlasapi.entity.util.Resolved;
import org.atlasapi.messaging.ResourceUpdatedMessage;
import org.atlasapi.organisation.Organisation;
import org.atlasapi.organisation.OrganisationResolver;
import org.atlasapi.organisation.OrganisationWriter;
import org.joda.time.DateTime;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class OrganisationBootstrapWorkerTest {

    private @Mock OrganisationResolver resolver;
    private @Mock OrganisationWriter writer;
    private @Mock ResourceRef updatedResource;
    private @Mock Organisation organisation;

    private ResourceUpdatedMessage message;
    private OrganisationBootstrapWorker worker;

    @Before
    public void setUp() {
        message = new ResourceUpdatedMessage(
                "message", Timestamp.of(DateTime.now()), updatedResource
        );
        worker = OrganisationBootstrapWorker.create(
                resolver,
                writer,
                "prefix",
                new MetricRegistry(),
                null
        );

        Id id = Id.valueOf(0L);
        when(updatedResource.getId()).thenReturn(id);
        when(resolver.resolveIds(ImmutableList.of(id)))
                .thenReturn(Futures.immediateFuture(
                        Resolved.valueOf(ImmutableList.of(organisation))
                ));
    }

    @Test
    public void testProcess() throws Exception {
        worker.process(message);
        verify(writer).write(organisation);
    }
}
