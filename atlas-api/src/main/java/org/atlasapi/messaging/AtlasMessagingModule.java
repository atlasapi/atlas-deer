package org.atlasapi.messaging;

import javax.jms.ConnectionFactory;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Primary;
import org.springframework.jms.connection.CachingConnectionFactory;

@Configuration
public class AtlasMessagingModule {
    
    @Value("${messaging.broker.url}") private String brokerUrl;
    @Value("${messaging.system}") private String messagingSystem;
    
    @Value("${messaging.destination.content.changes}") public String contentChanges;
    @Value("${messaging.destination.topics.changes}") public String topicChanges;

    @Bean @Primary
    public QueueFactory queueHelper() {
        return new QueueFactory(activemqConnectionFactory(), messagingSystem, serializer());
    }
    
    @Bean
    public MessageSerializer serializer() {
        return new JacksonMessageSerializer();
    }
    
    @Bean
    @Lazy(true)
    public ConnectionFactory activemqConnectionFactory() {
        ActiveMQConnectionFactory activeMQConnectionFactory = new ActiveMQConnectionFactory(brokerUrl);
        CachingConnectionFactory cachingConnectionFactory = new CachingConnectionFactory(activeMQConnectionFactory);
        return cachingConnectionFactory;
    }
    
    @Bean
    @Lazy(true)
    public MessageSender contentChanges() {
        return queueHelper().makeMessageSender(contentChanges);
    }
    
    @Bean
    @Lazy(true)
    public MessageSender topicChanges() {
        return queueHelper().makeMessageSender(topicChanges);
    }
    
//    @Bean 
//    @Lazy(true)
//    public MessageReplayer messageReplayer() {
//        return new MessageReplayer(messageStore, new JmsTemplate(activemqConnectionFactory()));
//    }
}
