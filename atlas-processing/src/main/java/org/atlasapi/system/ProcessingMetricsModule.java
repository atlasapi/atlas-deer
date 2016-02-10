package org.atlasapi.system;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.metabroadcast.common.properties.Configurer;

import com.codahale.metrics.Clock;
import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.MetricSet;
import com.codahale.metrics.graphite.GraphiteReporter;
import com.codahale.metrics.graphite.GraphiteUDP;
import com.codahale.metrics.jvm.GarbageCollectorMetricSet;
import com.codahale.metrics.jvm.MemoryUsageGaugeSet;
import com.codahale.metrics.jvm.ThreadStatesGaugeSet;
import org.springframework.context.annotation.Configuration;

@Configuration
public class ProcessingMetricsModule extends MetricsModule {

    private final String environmentPrefix = Configurer.get("metrics.environment.prefix").get();
    private final String graphiteHost = Configurer.get("metrics.graphite.host").get();
    private final int graphitePort = Configurer.get("metrics.graphite.port").toInt();

    @Override
    public MetricRegistry metrics() {
        MetricRegistry metrics = new MetricRegistry();
        registerMetrics("gc.", new GarbageCollectorMetricSet(), metrics);
        registerMetrics("memory.", new MemoryUsageGaugeSet(), metrics);
        registerMetrics("threads.", new ThreadStatesGaugeSet(), metrics);
        graphiteReporterFor(metrics);
        return metrics;
    }

    private void registerMetrics(String prefix, MetricSet metrics, MetricRegistry registry) {
        for (Map.Entry<String, Metric> metric : metrics.getMetrics().entrySet()) {
            registry.register(prefix.concat(metric.getKey()), metric.getValue());
        }
    }

    private GraphiteReporter graphiteReporterFor(MetricRegistry metrics) {
        GraphiteReporter reporter = GraphiteReporter.forRegistry(metrics)
                .prefixedWith("atlas.deer." + environmentPrefix + ".")
                .withClock(new Clock.UserTimeClock())
                .build(new GraphiteUDP(graphiteHost, graphitePort));
        reporter.start(1, TimeUnit.MINUTES);
        return reporter;
    }
}
