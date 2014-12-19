package org.atlasapi.system;

import java.util.Collection;
import java.util.Map;

import javax.annotation.PostConstruct;

import org.atlasapi.AtlasPersistenceModule;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.ImmutableList;
import com.metabroadcast.common.health.HealthProbe;
import com.metabroadcast.common.health.ProbeResult;
import com.metabroadcast.common.health.probes.DiskSpaceProbe;
import com.metabroadcast.common.health.probes.MemoryInfoProbe;
import com.metabroadcast.common.webapp.health.HealthController;
import com.metabroadcast.common.webapp.health.probes.MetricsProbe;
import com.netflix.astyanax.connectionpool.ConnectionPoolMonitor;
import com.netflix.astyanax.connectionpool.Host;
import com.netflix.astyanax.connectionpool.HostStats;

@Configuration
public class HealthModule {

    private final ImmutableList<HealthProbe> systemProbes = ImmutableList.of(
            new MemoryInfoProbe(),
            new DiskSpaceProbe()
    );

    private @Autowired Collection<HealthProbe> probes;
    private @Autowired HealthController healthController;
    private @Autowired AtlasPersistenceModule persistenceModule;

    public @Bean HealthController healthController() {
        return new HealthController(systemProbes);
    }

    public @Bean org.atlasapi.system.HealthController threadController() {
        return new org.atlasapi.system.HealthController();
    }

    public @Bean HealthProbe metricsProbe() {
        return new MetricsProbe("Metrics", metrics());
    }

    public @Bean HealthProbe astyanaxProbe() {
        return new HealthProbe() {

            @Override public ProbeResult probe() throws Exception {
                ProbeResult result = new ProbeResult(title());
                ConnectionPoolMonitor pool = persistenceModule.persistenceModule()
                        .getContext()
                        .getConnectionPoolMonitor();
                for (Map.Entry<Host, HostStats> hostEntry : pool.getHostStats().entrySet()) {
                    result.addInfo(hostEntry.getKey().toString(), hostEntry.getValue().toString());
                }
                result.addInfo("Socket timeouts", Long.toString(pool.getSocketTimeoutCount()));
                result.addInfo("Transport errors", Long.toString(pool.getTransportErrorCount()));
                result.addInfo("Pool-exhausted timeouts", Long.toString(pool.getPoolExhaustedTimeoutCount()));
                result.addInfo("Connections opened", Long.toString(pool.getConnectionCreatedCount()));
                result.addInfo("Connections closed", Long.toString(pool.getConnectionClosedCount()));
                return result;
            }

            @Override public String title() {
                return "Astyanax";
            }

            @Override public String slug() {
                return "astyanax";
            }
        };
    }

    public @Bean MetricRegistry metrics() {
        return new MetricRegistry();
    }

    @PostConstruct
    public void addProbes() {
        healthController.addProbes(probes);
    }
}