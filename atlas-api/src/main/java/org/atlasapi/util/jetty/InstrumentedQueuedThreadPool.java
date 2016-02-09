package org.atlasapi.util.jetty;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.RatioGauge;
import com.codahale.metrics.SlidingTimeWindowReservoir;
import org.eclipse.jetty.util.annotation.Name;
import org.eclipse.jetty.util.thread.QueuedThreadPool;

import static com.codahale.metrics.MetricRegistry.name;

/**
 * A copy of {@link com.codahale.metrics.jetty9.InstrumentedQueuedThreadPool} but with {@link
 * Histogram}s created for metrics.
 *
 * @author tom
 */
public class InstrumentedQueuedThreadPool extends QueuedThreadPool implements Runnable {

    private static final ScheduledExecutorService STATS_SAMPLING_EXECUTOR_SERVICE = new ScheduledThreadPoolExecutor(
            1);

    private final MetricRegistry metricRegistry;
    private final int samplingPeriodMinutes;
    private RatioGauge utilizationGauge;
    private Gauge<Integer> statsGauge;
    private Gauge<Integer> jobsGauge;

    private Histogram jobsHistogram;
    private Histogram sizeHistogram;
    private Histogram utilizationHistogram;

    public InstrumentedQueuedThreadPool(@Name("registry") MetricRegistry registry,
            int samplingPeriodMinutes) {
        this(registry, samplingPeriodMinutes, 200);
    }

    public InstrumentedQueuedThreadPool(@Name("registry") MetricRegistry registry,
            int samplingPeriodMinutes,
            @Name("maxThreads") int maxThreads) {
        this(registry, samplingPeriodMinutes, maxThreads, 8);
    }

    public InstrumentedQueuedThreadPool(@Name("registry") MetricRegistry registry,
            int samplingPeriodMinutes,
            @Name("maxThreads") int maxThreads,
            @Name("minThreads") int minThreads) {
        this(registry, samplingPeriodMinutes, maxThreads, minThreads, 60000);
    }

    public InstrumentedQueuedThreadPool(@Name("registry") MetricRegistry registry,
            int samplingPeriodMinutes,
            @Name("maxThreads") int maxThreads,
            @Name("minThreads") int minThreads,
            @Name("idleTimeout") int idleTimeout) {
        this(registry, samplingPeriodMinutes, maxThreads, minThreads, idleTimeout, null);
    }

    public InstrumentedQueuedThreadPool(@Name("registry") MetricRegistry registry,
            int samplingPeriodMinutes,
            @Name("maxThreads") int maxThreads,
            @Name("minThreads") int minThreads,
            @Name("idleTimeout") int idleTimeout,
            @Name("queue") BlockingQueue<Runnable> queue) {
        super(maxThreads, minThreads, idleTimeout, queue);
        this.metricRegistry = registry;
        this.samplingPeriodMinutes = samplingPeriodMinutes;
    }

    @Override
    protected void doStart() throws Exception {
        super.doStart();

        createUtilizationStats();
        createSizeStats();
        createJobsStats();

        STATS_SAMPLING_EXECUTOR_SERVICE.scheduleAtFixedRate(this, 1, 10, TimeUnit.SECONDS);
    }

    private void createUtilizationStats() {
        utilizationHistogram = new Histogram(createReservoir());
        metricRegistry.register(
                name(getName(), "utilization-percentage-histogram"),
                utilizationHistogram
        );
        utilizationGauge = new RatioGauge() {

            @Override
            protected Ratio getRatio() {
                return Ratio.of(getThreads() - getIdleThreads(), getThreads());
            }
        };

        metricRegistry.register(name(getName(), "utilization-percentage"), utilizationGauge);
    }

    private void createSizeStats() {
        sizeHistogram = new Histogram(createReservoir());
        metricRegistry.register(name(getName(), "size-histogram"), sizeHistogram);
        statsGauge = new Gauge<Integer>() {

            @Override
            public Integer getValue() {
                return getThreads();
            }
        };
        metricRegistry.register(name(getName(), "size"), statsGauge);
    }

    private void createJobsStats() {
        jobsHistogram = new Histogram(createReservoir());
        metricRegistry.register(name(getName(), "jobs-histogram"), jobsHistogram);
        jobsGauge = new Gauge<Integer>() {

            @Override
            public Integer getValue() {
                // This assumes the QueuedThreadPool is using a BlockingArrayQueue or
                // ArrayBlockingQueue for its queue, and is therefore a constant-time operation.
                return getQueue().size();
            }
        };
        metricRegistry.register(name(getName(), "jobs"), jobsGauge);
    }

    private SlidingTimeWindowReservoir createReservoir() {
        return new SlidingTimeWindowReservoir(samplingPeriodMinutes, TimeUnit.MINUTES);
    }

    @Override
    public void run() {

        jobsHistogram.update(jobsGauge.getValue());
        utilizationHistogram.update((long) (utilizationGauge.getValue() * 100));
        sizeHistogram.update(statsGauge.getValue());

    }

}