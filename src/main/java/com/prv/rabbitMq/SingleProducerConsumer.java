package com.prv.rabbitMq;

import com.prv.rabbitMq.message.MessageConsumer;
import com.prv.rabbitMq.message.QueueMessageProducer;
import com.prv.rabbitMq.service.RabbitService;

public class SingleProducerConsumer {

	public static void main(String[] args) throws InterruptedException {
		String serverUri = "amqp://admin:admin@192.168.99.100:5672/my_vhost";
		String queueName = "Testqueue1";

		RabbitService rabbitService = new RabbitService(serverUri);
		rabbitService.deleteQueue(queueName);
		rabbitService.createQueue(queueName);

		QueueMessageProducer producer = new QueueMessageProducer(queueName, serverUri);
		Thread producerThread = new Thread(producer);
		producerThread.start();

		Thread.sleep(3000);

		MessageConsumer consumer = new MessageConsumer(serverUri, queueName);
		Thread consumerThread = new Thread(consumer);
		consumerThread.start();
	}
}
