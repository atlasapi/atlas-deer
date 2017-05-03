package org.atlasapi.system;

import java.util.Collection;

import javax.annotation.PostConstruct;

import com.metabroadcast.common.health.probes.MongoProbe;
import com.metabroadcast.common.health.probes.Probe;
import com.mongodb.MongoClient;
import org.atlasapi.AtlasPersistenceModule;
import org.atlasapi.system.health.AstyanaxProbe;

import com.metabroadcast.common.health.HealthProbe;
import com.metabroadcast.common.health.probes.MemoryInfoProbe;
import com.metabroadcast.common.persistence.cassandra.health.CassandraProbe;
import com.metabroadcast.common.webapp.health.HealthController;
import com.metabroadcast.common.webapp.health.probes.MetricsProbe;

import com.google.common.collect.ImmutableList;
import org.atlasapi.system.health.probes.ElasticsearchProbe;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class HealthModule {

    private static final String METRIC_PREFIX = "atlas-deer-api";

    private @Autowired Collection<HealthProbe> probes;
    private @Autowired HealthController healthController;
    private @Autowired MetricsModule metricsModule;
    private @Autowired AtlasPersistenceModule persistenceModule;

    @Bean
    public HealthController healthController() {
        return new HealthController(ImmutableList.of(
                new MemoryInfoProbe()
        ));
    }

    @Bean
    public org.atlasapi.system.HealthController threadController() {
        return new org.atlasapi.system.HealthController(
                persistenceModule.persistenceModule().getSession()
        );
    }

    @Bean
    public org.atlasapi.system.health.HealthController apiK8HealthController() {
        return org.atlasapi.system.health.HealthController.create(getProbes(METRIC_PREFIX));
    }

    @PostConstruct
    public void addProbes() {
        healthController.addProbes(ImmutableList.of(
                new CassandraProbe(persistenceModule.persistenceModule().getSession()),
                new AstyanaxProbe(persistenceModule.persistenceModule().getContext())
        ));
        healthController.addProbes(probes);
        healthController.addProbes(ImmutableList.of(
                new MetricsProbe("Metrics", metricsModule.metrics())
        ));
    }

    Iterable<Probe> getProbes(String metricPrefix) {

        Probe cassandraProbe = com.metabroadcast.common.health.probes.CassandraProbe.create(
                "cassandra",
                persistenceModule.persistenceModule().getSession()
        );

        Probe astyanaxProbe = org.atlasapi.system.health.probes.AstyanaxProbe.create(
                "astyanax",
                persistenceModule.persistenceModule().getContext()
        );

        Probe mongoProbe = MongoProbe.create(
                "mongo",
                (MongoClient) persistenceModule.databasedReadMongo().database().getMongo()
        );

        Probe elasticSearchProbe = ElasticsearchProbe.create(
                "elasticsearch",
                persistenceModule.esContentIndexModule().getEsClient()
        );

        return ImmutableList.of(
                metricProbeFor(cassandraProbe, metricPrefix),
                metricProbeFor(astyanaxProbe, metricPrefix),
                metricProbeFor(mongoProbe, metricPrefix),
                metricProbeFor(elasticSearchProbe, metricPrefix)
        );
    }

    private com.metabroadcast.common.health.probes.MetricsProbe metricProbeFor(
            Probe probe,
            String metricPrefix
    ) {
        return com.metabroadcast.common.health.probes.MetricsProbe.builder()
                .withIdentifier(probe.getIdentifier() + "Metrics")
                .withDelegate(probe)
                .withMetricRegistry(metricsModule.metrics())
                .withMetricPrefix(metricPrefix)
                .build();
    }
}
