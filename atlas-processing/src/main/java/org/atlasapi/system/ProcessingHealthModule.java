package org.atlasapi.system;

import com.codahale.metrics.Clock;
import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.MetricSet;
import com.codahale.metrics.graphite.GraphiteReporter;
import com.codahale.metrics.graphite.GraphiteUDP;
import com.codahale.metrics.jvm.GarbageCollectorMetricSet;
import com.codahale.metrics.jvm.MemoryUsageGaugeSet;
import com.codahale.metrics.jvm.ThreadStatesGaugeSet;
import com.google.common.collect.ImmutableList;
import com.metabroadcast.common.health.HealthProbe;
import com.metabroadcast.common.health.ProbeResult;
import com.metabroadcast.common.health.probes.DiskSpaceProbe;
import com.metabroadcast.common.health.probes.MemoryInfoProbe;
import com.metabroadcast.common.properties.Configurer;
import com.metabroadcast.common.webapp.health.HealthController;
import com.metabroadcast.common.webapp.health.probes.MetricsProbe;
import com.netflix.astyanax.connectionpool.ConnectionPoolMonitor;
import org.atlasapi.AtlasPersistenceModule;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.annotation.PostConstruct;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@Configuration
public class ProcessingHealthModule extends HealthModule {

    private final ImmutableList<HealthProbe> systemProbes = ImmutableList.of(
            new MemoryInfoProbe(),
            new DiskSpaceProbe()
    );

    private @Autowired Collection<HealthProbe> probes;
    private @Autowired HealthController healthController;
    private @Autowired AtlasPersistenceModule persistenceModule;
    private final String environmentPrefix =  Configurer.get("metrics.environment.prefix").get();
    private final String graphiteHost =  Configurer.get("metrics.graphite.host").get();
    private final int graphitePort =  Configurer.get("metrics.graphite.port").toInt();

    public @Bean HealthController healthController() {
        return new HealthController(systemProbes);
    }

    public @Bean org.atlasapi.system.HealthController threadController() {
        return new org.atlasapi.system.HealthController(persistenceModule.persistenceModule().getSession());
    }

    public GraphiteReporter graphiteReporterFor(MetricRegistry metrics) {
        GraphiteReporter reporter = GraphiteReporter.forRegistry(metrics)
                .prefixedWith("atlas.deer." + environmentPrefix + ".")
                .withClock(new Clock.UserTimeClock())
                .build(new GraphiteUDP(graphiteHost, graphitePort));
        reporter.start(1, TimeUnit.MINUTES);
        return reporter;
    }

    public @Bean HealthProbe metricsProbe() {
        return new MetricsProbe("Metrics", metrics());
    }

    public @Bean HealthProbe astyanaxProbe() {
        return new HealthProbe() {

            @Override public ProbeResult probe() throws Exception {
                ProbeResult result = new ProbeResult(title());
                ConnectionPoolMonitor pool = persistenceModule.persistenceModule()
                        .getContext().getConnectionPoolMonitor();
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
        MetricRegistry metrics = new MetricRegistry();
        registerMetrics("gc.", new GarbageCollectorMetricSet(), metrics);
        registerMetrics("memory.", new MemoryUsageGaugeSet(), metrics);
        registerMetrics("threads.", new ThreadStatesGaugeSet(), metrics);
        graphiteReporterFor(metrics);
        return metrics;
    }

    @PostConstruct
    public void addProbes() {
        healthController.addProbes(ImmutableList.of(metricsProbe()));
    }

    private void registerMetrics(String prefix, MetricSet metrics, MetricRegistry registry) {
        for (Map.Entry<String, Metric> metric : metrics.getMetrics().entrySet()) {
            registry.register(prefix.concat(metric.getKey()), metric.getValue());
        }
    }
}