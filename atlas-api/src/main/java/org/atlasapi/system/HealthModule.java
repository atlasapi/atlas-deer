package org.atlasapi.system;

import java.util.Collection;

import javax.annotation.PostConstruct;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.ImmutableList;
import com.metabroadcast.common.health.HealthProbe;
import com.metabroadcast.common.health.probes.DiskSpaceProbe;
import com.metabroadcast.common.health.probes.MemoryInfoProbe;
import com.metabroadcast.common.webapp.health.HealthController;
import com.metabroadcast.common.webapp.health.probes.MetricsProbe;

@Configuration
public class HealthModule {

    private final ImmutableList<HealthProbe> systemProbes = ImmutableList.of(
            new MemoryInfoProbe(),
            new DiskSpaceProbe()
    );

    private @Autowired Collection<HealthProbe> probes;
    private @Autowired HealthController healthController;

    public @Bean HealthController healthController() {
        return new HealthController(systemProbes);
    }

    public @Bean org.atlasapi.system.HealthController threadController() {
        return new org.atlasapi.system.HealthController();
    }

    public @Bean HealthProbe metricsProbe() {
        return new MetricsProbe(metrics());
    }

    public @Bean MetricRegistry metrics() {
        return new MetricRegistry();
    }

    @PostConstruct
    public void addProbes() {
        healthController.addProbes(ImmutableList.of(metricsProbe()));
        healthController.addProbes(probes);
    }
}