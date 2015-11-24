package org.atlasapi.system;

import java.util.Collection;

import javax.annotation.PostConstruct;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.ImmutableList;
import com.metabroadcast.common.health.HealthProbe;
import com.metabroadcast.common.health.probes.MemoryInfoProbe;
import com.metabroadcast.common.webapp.health.HealthController;
import com.metabroadcast.common.webapp.health.probes.MetricsProbe;

@Configuration
public class HealthModule {

    private static final MetricRegistry metrics = new MetricRegistry();

    private @Autowired Collection<HealthProbe> probes;
    private @Autowired HealthController healthController;
    // TODO There is a circular dependency here. Extract metrics to their own modules
    // private @Autowired AtlasPersistenceModule persistenceModule;

    public @Bean HealthController healthController() {
        return new HealthController(ImmutableList.of(
                new MemoryInfoProbe()
        ));
    }

    public @Bean org.atlasapi.system.HealthController threadController() {
        return new org.atlasapi.system.HealthController(null);
    }

    public @Bean HealthProbe metricsProbe() {
        return new MetricsProbe("Metrics", metrics());
    }

    public @Bean MetricRegistry metrics() {
        return metrics;
    }

    @PostConstruct
    public void addProbes() {
//        healthController.addProbes(ImmutableList.of(
//                new CassandraProbe(persistenceModule.persistenceModule().getSession())
//        ));
        healthController.addProbes(probes);
    }
}
