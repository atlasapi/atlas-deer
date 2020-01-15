package org.atlasapi.util;

import com.github.dikhan.pagerduty.client.events.PagerDutyEventsClient;
import com.github.dikhan.pagerduty.client.events.domain.EventResult;
import com.github.dikhan.pagerduty.client.events.domain.Payload;
import com.github.dikhan.pagerduty.client.events.domain.TriggerIncident;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

    public class PagerDutyClientWithKey {

        private static final Logger log = LoggerFactory.getLogger(PagerDutyClientWithKey.class);

        private final PagerDutyEventsClient pdClient;
        private final String integrationKey;
        private final boolean raiseIncidents;

        public PagerDutyClientWithKey(PagerDutyEventsClient pdClient, String apiKey, boolean raiseIncidents) {
            this.pdClient = pdClient;
            this.integrationKey = apiKey;
            this.raiseIncidents = raiseIncidents;
        }

        @Nullable
        public EventResult trigger(Payload payload, String dedupKey) {

            if(!raiseIncidents){
                return null;
            }

            try {
                TriggerIncident incident = TriggerIncident.TriggerIncidentBuilder
                        .newBuilder(
                                integrationKey,
                                payload
                        )
                        .setDedupKey(dedupKey)
                        .build();

                EventResult pdResult = pdClient.trigger(incident);
                log.info("Triggered PD alert. {}", pdResult.toString());

                return pdResult;
            } catch (Exception e) {
                log.error("Triggering PagerDuty incident failed for {}", dedupKey, e);
                return null;
            }

        }
    }