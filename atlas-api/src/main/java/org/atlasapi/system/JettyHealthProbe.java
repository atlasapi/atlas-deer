package org.atlasapi.system;

import java.util.Map;
import java.util.Map.Entry;

import javax.servlet.ServletContext;

import org.atlasapi.AtlasServer;

import com.metabroadcast.common.health.HealthProbe;
import com.metabroadcast.common.health.ProbeResult;

import org.springframework.web.context.ServletContextAware;

public class JettyHealthProbe implements HealthProbe, ServletContextAware {

    private ServletContext servletContext;

    @Override
    public ProbeResult probe() throws Exception {
        ProbeResult probeResult = new ProbeResult("Requests");
        Map<String, String> metrics = AtlasServer.getMetrics(
                servletContext.getAttribute(AtlasServer.CONTEXT_ATTRIBUTE));

        for (Entry<String, String> entry : metrics.entrySet()) {
            probeResult.addInfo(entry.getKey(), entry.getValue());
        }
        return probeResult;
    }

    @Override
    public String title() {
        return "Jetty";
    }

    @Override
    public String slug() {
        return "jetty";
    }

    @Override
    public void setServletContext(ServletContext servletContext) {
        this.servletContext = servletContext;
    }

}