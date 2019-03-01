package com.sk.newsbroker.service;

import org.springframework.amqp.core.AmqpTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import com.sk.newsbroker.model.Post;

@Service
public class RabbitMQSender {

	@Autowired
	private AmqpTemplate rabbitTemplate;
	
	@Value("${newsbroker.rabbitmq.exchange}")
	private String exchange;
	
	@Value("${newsbroker.rabbitmq.routingkey}")
	private String routingkey;
	
	public void send(Post post) {
		rabbitTemplate.convertAndSend(exchange,routingkey,post);
		System.out.println("Send Post Message:"+ post);
	}
}
