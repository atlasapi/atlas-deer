package org.atlasapi.system;

import javax.annotation.PostConstruct;

import org.atlasapi.AtlasPersistenceModule;
import org.atlasapi.system.health.AstyanaxProbe;

import com.metabroadcast.common.health.probes.MemoryInfoProbe;
import com.metabroadcast.common.persistence.cassandra.health.CassandraProbe;
import com.metabroadcast.common.webapp.health.HealthController;
import com.metabroadcast.common.webapp.health.probes.MetricsProbe;

import com.google.common.collect.ImmutableList;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class ProcessingHealthModule extends HealthModule {

    private static final String METRIC_PREFIX = "atlas-deer-processing";

    private @Autowired HealthController healthController;
    private @Autowired ProcessingMetricsModule metricsModule;
    private @Autowired AtlasPersistenceModule persistenceModule;

    public
    @Bean
    HealthController healthController() {
        return new HealthController(ImmutableList.of(
                new MemoryInfoProbe()
        ));
    }

    public
    @Bean
    org.atlasapi.system.HealthController threadController() {
        return new org.atlasapi.system.HealthController(persistenceModule.persistenceModule()
                .getSession());
    }

    @Bean
    public org.atlasapi.system.health.HealthController k8HealthController() {
        return org.atlasapi.system.health.HealthController.create(getProbes(METRIC_PREFIX));
    }

    @PostConstruct
    public void addProbes() {
        healthController.addProbes(ImmutableList.of(
                new CassandraProbe(persistenceModule.persistenceModule().getSession()),
                new AstyanaxProbe(persistenceModule.persistenceModule().getContext()),
                new MetricsProbe("Metrics", metricsModule.metrics())
        ));
    }

}