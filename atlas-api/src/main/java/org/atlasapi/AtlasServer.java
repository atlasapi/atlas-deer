package org.atlasapi;

import java.io.File;
import java.lang.reflect.InvocationTargetException;
import java.net.URL;
import java.text.DecimalFormat;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Executor;

import org.atlasapi.util.jetty.InstrumentedQueuedThreadPool;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMap.Builder;
import org.eclipse.jetty.io.ByteBufferPool;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.HttpConfiguration;
import org.eclipse.jetty.server.HttpConnectionFactory;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.util.thread.QueuedThreadPool;
import org.eclipse.jetty.util.thread.Scheduler;
import org.eclipse.jetty.webapp.WebAppContext;

public class AtlasServer {

    private static final String METRIC_METHOD_NAME = "getMetrics";
    private static final String SERVER_REQUEST_THREADS_OVERRIDE_PROPERTY_NAME = "request.threads";
    private static final int DEFAULT_SERVER_REQUEST_THREADS = 100;
    private static final String SERVER_REQUEST_THREAD_PREFIX = "api-request-thread";
    private static final int SERVER_ACCEPT_QUEUE_SIZE = 200;

    private static final String SERVER_PORT_OVERRIDE_PROPERTY_NAME = "server.port";

    private static final int MONITORING_REQUEST_THREADS = 20;
    private static final String MONITORING_REQUEST_THREAD_PREFIX = "monitoring-request-thread";
    private static final int MONITORING_DEFAULT_PORT = 8081;
    private static final String MONITORING_PORT_OVERRIDE_PROPERTY_NAME = "monitoring.port";

    public static final String CONTEXT_ATTRIBUTE = "ATLAS_MAIN";
    private static final String LOCAL_WAR_DIR = "./src/main/webapp/";

    private static final String SAMPLING_PERIOD_PROPERTY = "samplingPeriodMinutes";
    private static final int DEFAULT_SAMPLING_PERIOD_MINUTES = 3;

    private final MetricRegistry metrics = new MetricRegistry();

    public static void startWithMonitoringOnPort(int port) throws Exception {
        new AtlasServer(port, true);
    }

    public static void startOnPort(int port) throws Exception {
        new AtlasServer(port, false);
    }

    public AtlasServer(int port, boolean monitor) throws Exception {
        System.out.println(String.format(
                "port: %s, monitoring %s",
                port,
                monitor ? "enabled" : "disabled"
        ));
        this.start(port, monitor);
    }

    private void start(int port, boolean monitor) throws Exception {
        System.out.println(this.getClass().getName().replace(".", "/") + ".class");
        WebAppContext apiContext = createWebApp(
                warBase() + "WEB-INF/web.xml",
                createApiServer(port)
        );
        apiContext.setAttribute(CONTEXT_ATTRIBUTE, this);
        if (monitor) {
            WebAppContext monitoringContext = createWebApp(
                    warBase() + "WEB-INF/web-monitoring.xml",
                    createMonitoringServer()
            );
            monitoringContext.setAttribute(CONTEXT_ATTRIBUTE, this);
        }
    }

    private WebAppContext createWebApp(String descriptor, final Server server) throws Exception {
        System.out.println(String.format("Creating web app: %s", descriptor));
        WebAppContext ctx = new WebAppContext(warBase(), "/");
        ctx.setDescriptor(descriptor);
        server.setHandler(ctx);
        server.start();

        return ctx;
    }

    private String warBase() {
        if (new File(LOCAL_WAR_DIR).exists()) {
            return LOCAL_WAR_DIR;
        }
        String knownPath = this.getClass().getName().replace(".", "/") + ".class";
        URL url = this.getClass().getClassLoader().getResource(knownPath);
        return url.toString().replace(knownPath, "");
    }

    private Server createApiServer(int port) throws Exception {
        int requestThreads;
        String requestThreadsString = System.getProperty(
                SERVER_REQUEST_THREADS_OVERRIDE_PROPERTY_NAME);
        if (requestThreadsString == null) {
            requestThreads = DEFAULT_SERVER_REQUEST_THREADS;
        } else {
            requestThreads = Integer.parseInt(requestThreadsString);
        }

        return createServer(port, SERVER_PORT_OVERRIDE_PROPERTY_NAME, requestThreads,
                SERVER_ACCEPT_QUEUE_SIZE, SERVER_REQUEST_THREAD_PREFIX
        );
    }

    private Server createMonitoringServer() throws Exception {
        int defaultAcceptQueueSize = 0;
        return createServer(MONITORING_DEFAULT_PORT, MONITORING_PORT_OVERRIDE_PROPERTY_NAME,
                MONITORING_REQUEST_THREADS, defaultAcceptQueueSize, MONITORING_REQUEST_THREAD_PREFIX
        );
    }

    private Server createServer(int defaultPort, String portPropertyName, int maxThreads,
            int acceptQueueSize, String threadNamePrefix) {

        Server server = new Server(createRequestThreadPool(maxThreads, threadNamePrefix));
        createServerConnector(server, createHttpConnectionFactory(), defaultPort, portPropertyName,
                acceptQueueSize
        );

        return server;
    }

    private QueuedThreadPool createRequestThreadPool(int maxThreads, String threadNamePrefix) {
        QueuedThreadPool pool = new InstrumentedQueuedThreadPool(metrics, getSamplingPeriod(),
                maxThreads
        );
        pool.setName(threadNamePrefix);

        return pool;
    }

    private void createServerConnector(Server server, HttpConnectionFactory connectionFactory,
            int defaultPort, String portPropertyName, int acceptQueueSize) {

        int acceptors = Runtime.getRuntime().availableProcessors();
        Executor defaultExecutor = null;
        Scheduler defaultScheduler = null;
        ByteBufferPool defaultByteBufferPool = null;
        int selectors = 0;

        ServerConnector connector = new ServerConnector(server, defaultExecutor, defaultScheduler,
                defaultByteBufferPool, acceptors, selectors, connectionFactory
        );

        connector.setPort(getPort(defaultPort, portPropertyName));
        connector.setAcceptQueueSize(acceptQueueSize);
        server.setConnectors(new Connector[] { connector });
    }

    private HttpConnectionFactory createHttpConnectionFactory() {
        HttpConfiguration config = new HttpConfiguration();
        config.setRequestHeaderSize(8192);
        config.setResponseHeaderSize(1024);

        return new HttpConnectionFactory(config);
    }

    private int getPort(int defaultPort, String portProperty) {
        String customPort = System.getProperty(portProperty);
        if (customPort != null) {
            return Integer.parseInt(customPort);
        }
        return defaultPort;
    }

    private int getSamplingPeriod() {
        String customSamplingDuration = System.getProperty(SAMPLING_PERIOD_PROPERTY);
        if (customSamplingDuration != null) {
            return Integer.parseInt(customSamplingDuration);
        }
        return DEFAULT_SAMPLING_PERIOD_MINUTES;
    }

    public Map<String, String> getMetrics() {
        DecimalFormat dpsFormat = new DecimalFormat("0.00");

        Builder<String, String> metricsResults = ImmutableMap.builder();
        for (Entry<String, Histogram> entry : metrics.getHistograms().entrySet()) {
            metricsResults.put(
                    entry.getKey() + "-mean",
                    dpsFormat.format(entry.getValue().getSnapshot().getMean())
            );
            metricsResults.put(
                    entry.getKey() + "-max",
                    Long.toString(entry.getValue().getSnapshot().getMax())
            );
            metricsResults.put(
                    entry.getKey() + "-99th",
                    dpsFormat.format(entry.getValue().getSnapshot().get99thPercentile())
            );
        }

        return metricsResults.build();
    }

    @SuppressWarnings("unchecked")
    public static Map<String, String> getMetrics(Object atlasServer)
            throws IllegalArgumentException,
            SecurityException, IllegalAccessException, InvocationTargetException {
        Class<? extends Object> clazz = atlasServer.getClass();
        if (clazz.getCanonicalName() != AtlasServer.class.getCanonicalName()) {
            throw new IllegalArgumentException("Parameter must be instance of "
                    + AtlasServer.class.getCanonicalName());
        }

        try {
            return (Map<String, String>) clazz.getDeclaredMethod(METRIC_METHOD_NAME)
                    .invoke(atlasServer);
        } catch (NoSuchMethodException e) {
            throw new IllegalArgumentException(
                    "Couldn't find method "
                            + METRIC_METHOD_NAME
                            + ": Perhaps a mismatch between AtlasMain objects across classloaders?",
                    e
            );
        }
    }

}
