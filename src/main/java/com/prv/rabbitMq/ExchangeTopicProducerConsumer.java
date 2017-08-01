package com.prv.rabbitMq;

import com.prv.rabbitMq.message.ExchangeMessageProducer;
import com.prv.rabbitMq.message.MessageConsumer;
import com.prv.rabbitMq.service.RabbitService;

public class ExchangeTopicProducerConsumer {

	public static void main(String[] args) {
		String serverUri = "amqp://admin:admin@192.168.99.100:5672/my_vhost";
		String exchangeName = "wdp-test";
		String queueName = "policy-queue";
		String bindingKey = "v2.#";
		String routingKey = "v2.create";

		RabbitService rabbitService = new RabbitService(serverUri);
		rabbitService.createExchangeAndQueueAndBindQueue(exchangeName, queueName, bindingKey);

		ExchangeMessageProducer producer = new ExchangeMessageProducer(serverUri, exchangeName, routingKey);
		Thread producerThread = new Thread(producer);
		producerThread.start();

		MessageConsumer consumer = new MessageConsumer(serverUri, queueName);
		Thread consumerThread = new Thread(consumer);
		consumerThread.start();
	}

}
