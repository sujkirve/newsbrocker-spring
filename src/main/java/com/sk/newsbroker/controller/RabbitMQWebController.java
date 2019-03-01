package com.sk.newsbroker.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.sk.newsbroker.model.Post;
import com.sk.newsbroker.service.RabbitMQReceiver;
import com.sk.newsbroker.service.RabbitMQSender;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@RestController
@RequestMapping(value = "/api/")
public class RabbitMQWebController {

	@Autowired
	RabbitMQSender rabbitMQSender;

	@Autowired
	RabbitMQReceiver rabbitMQReceiver;

	@PostMapping(value = "/news")
	public Mono<ResponseEntity<?>> producer(@RequestBody(required=true) Post postInput) {

		return Mono.fromCallable(() -> {
			rabbitMQSender.send(postInput);
			return ResponseEntity.accepted().build();
		});

	}

	@GetMapping(value = "/news",produces = MediaType.TEXT_EVENT_STREAM_VALUE)
	public Flux<?> receiveMessagesFromQueue() {
		return rabbitMQReceiver.receiveMessage();

	}
}
