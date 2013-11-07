package org.atlasapi.messaging;

import javax.jms.ConnectionFactory;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Lazy;
import org.springframework.jms.connection.CachingConnectionFactory;
import org.springframework.jms.core.JmsTemplate;

@Configuration
public class AtlasMessagingModule {
    
    @Value("${messaging.broker.url}")
    private String brokerUrl;
    @Value("${messaging.destination.content.changes}")
    private String contentChangesDestination;
    @Value("${messaging.destination.topics.changes}")
    private String topicChangesDestination;
//    @Autowired
//    private MessageStore messageStore;

    @Bean
    @Lazy(true)
    public ConnectionFactory activemqConnectionFactory() {
        ActiveMQConnectionFactory activeMQConnectionFactory = new ActiveMQConnectionFactory(brokerUrl);
        CachingConnectionFactory cachingConnectionFactory = new CachingConnectionFactory(activeMQConnectionFactory);
        return cachingConnectionFactory;
    }
    
    @Bean
    @Lazy(true)
    public JmsTemplate contentChanges() {
        JmsTemplate jmsTemplate = new JmsTemplate(activemqConnectionFactory());
        jmsTemplate.setPubSubDomain(true);
        jmsTemplate.setDefaultDestinationName(contentChangesDestination);
        return jmsTemplate;
    }
    
    @Bean
    @Lazy(true)
    public JmsTemplate topicChanges() {
        JmsTemplate jmsTemplate = new JmsTemplate(activemqConnectionFactory());
        jmsTemplate.setPubSubDomain(true);
        jmsTemplate.setDefaultDestinationName(topicChangesDestination);
        return jmsTemplate;
    }
    
//    @Bean 
//    @Lazy(true)
//    public MessageReplayer messageReplayer() {
//        return new MessageReplayer(messageStore, new JmsTemplate(activemqConnectionFactory()));
//    }
}
