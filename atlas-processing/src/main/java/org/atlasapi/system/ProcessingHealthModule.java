package org.atlasapi.system;

import java.util.Collection;

import javax.annotation.PostConstruct;

import org.atlasapi.AtlasPersistenceModule;
import org.atlasapi.system.health.AstyanaxProbe;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.google.common.collect.ImmutableList;
import com.metabroadcast.common.health.HealthProbe;
import com.metabroadcast.common.health.probes.MemoryInfoProbe;
import com.metabroadcast.common.persistence.cassandra.health.CassandraProbe;
import com.metabroadcast.common.webapp.health.HealthController;
import com.metabroadcast.common.webapp.health.probes.MetricsProbe;

@Configuration
public class ProcessingHealthModule extends HealthModule {

    private @Autowired Collection<HealthProbe> probes;
    private @Autowired HealthController healthController;
    private @Autowired ProcessingMetricsModule metricsModule;
    private @Autowired AtlasPersistenceModule persistenceModule;

    public @Bean HealthController healthController() {
        return new HealthController(ImmutableList.of(
                new MemoryInfoProbe()
        ));
    }

    public @Bean org.atlasapi.system.HealthController threadController() {
        return new org.atlasapi.system.HealthController(persistenceModule.persistenceModule().getSession());
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