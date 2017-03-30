package org.atlasapi.system.health;

import com.metabroadcast.common.health.Health;
import com.metabroadcast.common.health.Result;
import com.metabroadcast.common.health.Status;
import com.metabroadcast.common.health.probes.Probe;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;

import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.PrintWriter;

import static com.google.common.base.Preconditions.checkNotNull;
import static javax.servlet.http.HttpServletResponse.SC_INTERNAL_SERVER_ERROR;
import static javax.servlet.http.HttpServletResponse.SC_OK;

@Controller
public class HealthController {

    private final Health health;

    private HealthController(Iterable<Probe> probes) {
        this.health = Health.create(checkNotNull(probes));
    }

    public static HealthController create(Iterable<Probe> probes) {
        return new HealthController(probes);
    }

    @RequestMapping("/system/health/alive")
    public void isAlive(HttpServletResponse response) {
        response.setStatus(HttpServletResponse.SC_OK);
    }

    @RequestMapping("/system/health/probes")
    public void showHealthForProbes(HttpServletResponse response) throws IOException {
        Result result = health.status(Health.FailurePolicy.ANY);
        response.setStatus(result.getStatus() == Status.HEALTHY ? SC_OK
                                                                : SC_INTERNAL_SERVER_ERROR
        );

        PrintWriter writer = response.getWriter();
        result.getProbeResults().forEach(probeResult -> {
                writer.println(
                        String.format(
                                "%s: %s",
                                probeResult.getIdentifier(),
                                probeResult.getStatus().toString()
                        )
                );
                probeResult.getMsg().ifPresent(writer::println);
                probeResult.getReason().ifPresent(writer::println);
                writer.println();
        });
    }
}
