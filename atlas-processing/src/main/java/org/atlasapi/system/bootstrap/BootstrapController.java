package org.atlasapi.system.bootstrap;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;

import javax.servlet.http.HttpServletResponse;

import org.atlasapi.entity.Identifiable;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.elasticsearch.common.base.Throwables;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;

@Controller
public class BootstrapController {

    private static final Log log = LogFactory.getLog(BootstrapController.class);
    private final ExecutorService scheduler = Executors.newFixedThreadPool(Runtime.getRuntime()
            .availableProcessors() * 10);
    private final ObjectMapper jsonMapper = new ObjectMapper();

    private static class Bootstrappable<R extends Identifiable> {

        private final ResourceBootstrapper<R> bootstrapper;
        private final BootstrapListenerFactory<? super R> listener;

        public Bootstrappable(ResourceBootstrapper<R> bootstrapper,
                BootstrapListenerFactory<? super R> listener) {
            this.bootstrapper = bootstrapper;
            this.listener = listener;
        }

        public ResourceBootstrapper<R> getBootstrapper() {
            return bootstrapper;
        }

        public BootstrapListenerFactory<? super R> getListener() {
            return listener;
        }

    }

    private final Map<String, Bootstrappable<?>> bootstrappables = Maps.newHashMap();

    public <R extends Identifiable> BootstrapController addBootstrapPair(String name,
            ResourceBootstrapper<R> bootstrapper, BootstrapListenerFactory<? super R> listener) {
        bootstrappables.put(name, new Bootstrappable<R>(bootstrapper, listener));
        return this;
    }

    @RequestMapping(method = RequestMethod.GET, value = "/system/bootstrap/{id}")
    public <T extends Identifiable> void bootstrap(@PathVariable("id") String id,
            @RequestParam(required = false) String concurrency, HttpServletResponse response)
            throws IOException {
        @SuppressWarnings("unchecked")
        Bootstrappable<T> bootstrappable = (Bootstrappable<T>) bootstrappables.get(id);
        int concurrencyLevel = getConcurrencyLevel(concurrency, response);
        BootstrapListener<? super T> listener = bootstrappable.getListener()
                .buildWithConcurrency(concurrencyLevel);
        doBootstrap(bootstrappable.getBootstrapper(), listener, response);
    }

    @RequestMapping(method = RequestMethod.GET, value = "/system/bootstrap/{id}/status")
    public void boostrapStatus(@PathVariable("id") String id, HttpServletResponse response)
            throws IOException {
        writeBootstrapStatus(bootstrappables.get(id).getBootstrapper(), response);
    }

    private <T extends Identifiable> void doBootstrap(
            final ResourceBootstrapper<T> contentBootstrapper,
            final BootstrapListener<? super T> changeListener, HttpServletResponse response)
            throws IOException {
        try {
            scheduler.submit(new Runnable() {

                @Override
                public void run() {
                    boolean bootstrapping = contentBootstrapper.loadAllIntoListener(changeListener);
                    if (!bootstrapping) {
                        log.warn(
                                "Bootstrapping failed because apparently busy bootstrapping something else.");
                    }
                }
            });
        } catch (RejectedExecutionException ex) {
            response.sendError(
                    HttpServletResponse.SC_SERVICE_UNAVAILABLE,
                    "Currently bootstrapping another component."
            );
        }
    }

    public void writeBootstrapStatus(ResourceBootstrapper<?> contentBootstrapper,
            HttpServletResponse response) throws IOException {
        Map<String, Object> result = Maps.newHashMap();
        result.put("bootstrapping", contentBootstrapper.isBootstrapping());
        result.put("lastStatus", contentBootstrapper.getLastStatus());
        Exception lastException = contentBootstrapper.getLastException();
        if (lastException != null) {
            result.put("lastException", Throwables.getStackTraceAsString(lastException));
        }
        if (contentBootstrapper.isBootstrapping()) {
            result.put("destination", contentBootstrapper.getDestination());
        }
        jsonMapper.writeValue(response.getOutputStream(), result);
        response.flushBuffer();
    }

    private int getConcurrencyLevel(String concurrency, HttpServletResponse response)
            throws IOException {
        int concurrencyLevel = 0;
        if (Strings.isNullOrEmpty(concurrency)) {
            concurrencyLevel = 1;
        } else {
            try {
                concurrencyLevel = Integer.parseInt(concurrency);
            } catch (NumberFormatException ex) {
                response.sendError(
                        HttpServletResponse.SC_BAD_REQUEST,
                        "Bad concurrency parameter!"
                );
            }
        }
        return concurrencyLevel;
    }
}
