package org.atlasapi.messaging;

import javax.jms.ConnectionFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jms.core.JmsTemplate;
import org.springframework.jms.listener.DefaultMessageListenerContainer;
import org.springframework.jms.listener.adapter.MessageListenerAdapter;

public final class QueueFactory {

    private static final Logger log = LoggerFactory.getLogger(QueueFactory.class);
    
    private final String system;
    private final ConnectionFactory cf;
    private final MessageSerializer serializer;

    public QueueFactory(ConnectionFactory cf, String system, MessageSerializer serializer) {
        this.cf = cf;
        this.system = system;
        this.serializer = serializer;
    }

    private String virtualTopicProducer(String name) {
        return String.format("VirtualTopic.%s.%s", system, name);
    }
    
    private String virtualTopicConsumer(String consumer, String producer) {
        return String.format("Consumer.%s.VirtualTopic.%s.%s", consumer, system, producer);
    }
    
    private String replayDestination(String name) {
        return String.format("%s.Replay.%s", name, system);
    }
    
    public DefaultMessageListenerContainer makeVirtualTopicConsumer(Worker worker, String consumer, String producer, int consumers, int maxConsumers) {
        return makeContainer(worker, virtualTopicConsumer(consumer, producer), consumers, maxConsumers);
    }

    public DefaultMessageListenerContainer makeReplayContainer(Worker worker, String name, int consumers, int maxConsumers) {
        return makeContainer(worker, replayDestination(name), consumers, maxConsumers);
    }
    
    private DefaultMessageListenerContainer makeContainer(Worker worker, String destination, int consumers, int maxConsumers) {
        log.info("Reading {} with {}", destination, worker.getClass().getSimpleName());
        
        JmsMessageAdapter messageAdapter = new JmsMessageAdapter(serializer, worker);
        MessageListenerAdapter adapter = new MessageListenerAdapter(messageAdapter);
        adapter.setDefaultListenerMethod("onMessage");

        DefaultMessageListenerContainer container = new DefaultMessageListenerContainer();
        container.setMessageListener(adapter);
        container.setConnectionFactory(cf);
        container.setDestinationName(destination);
        container.setConcurrentConsumers(consumers);
        container.setMaxConcurrentConsumers(maxConsumers);

        return container;
    }
    
    public MessageSender makeMessageSender(String destinationName) {
        String destination = this.virtualTopicProducer(destinationName);
        log.info("Writing {}", destination);
        JmsTemplate jmsTemplate = new JmsTemplate(cf);
        jmsTemplate.setPubSubDomain(true);
        jmsTemplate.setDefaultDestinationName(destination);
        return new JmsMessageSender(jmsTemplate, serializer);
    }
    
}
