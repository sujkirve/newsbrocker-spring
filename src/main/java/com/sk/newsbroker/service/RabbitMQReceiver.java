package com.sk.newsbroker.service;

import java.time.Duration;

import org.springframework.amqp.core.AmqpAdmin;
import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.Exchange;
import org.springframework.amqp.core.ExchangeBuilder;
import org.springframework.amqp.core.MessageListener;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.QueueBuilder;
import org.springframework.amqp.rabbit.listener.MessageListenerContainer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import com.sk.newsbroker.factory.MessageListenerContainerFactory;

import reactor.core.publisher.Flux;

@Service
public class RabbitMQReceiver {

	@Value("${newsbroker.rabbitmq.queue}")
	private String queue;
	
	@Value("${newsbroker.rabbitmq.exchange}")
	private String exchange;
	
	@Value("${newsbroker.rabbitmq.routingkey}")
	private String routingkey;
	
	@Autowired
	private MessageListenerContainerFactory messageListenerContainerFactory;
	
	@Autowired
	private AmqpAdmin amqpAdmin;
	
	public Flux<String> receiveMessage() {
		
		Queue topicQueue = createTopicQueue();
	    String qname = topicQueue.getName();
	    
		 MessageListenerContainer mlc = messageListenerContainerFactory
			      .createMessageListenerContainer(qname);
		 
		 Flux<String> f = Flux.<String> create(emitter -> {
		        mlc.setupMessageListener((MessageListener) m -> {
		            String payload = new String(m.getBody());
		            System.out.println(qname +"payload :"+payload);
		            emitter.next(payload);
		        });
		        emitter.onRequest(v -> {
		            mlc.start();
		        });
		        emitter.onDispose(() -> {
		            mlc.stop();
		        });
		      });
		 
		 return Flux.interval(Duration.ofSeconds(5))
			      .map(v -> "{No news is good news}")
			      .mergeWith(f);
		 
		 
	}
	
	
	
	private Queue createTopicQueue() {
		 
	    Exchange ex = ExchangeBuilder
	      .topicExchange(exchange)
	      .durable(true)
	      .build();
	    
	    amqpAdmin.declareExchange(ex);
	    Queue q = QueueBuilder
	      .nonDurable()
	      .build();     
	    amqpAdmin.declareQueue(q);
	    Binding b = BindingBuilder.bind(q)
	      .to(ex)
	      .with(routingkey)
	      .noargs();        
	    amqpAdmin.declareBinding(b);
	    return q;
	}
}
