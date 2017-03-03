package com.prv.rabbitMq;

import com.prv.rabbitMq.single.MessageConsumer;
import com.prv.rabbitMq.single.MessageProducer;
import com.prv.rabbitMq.single.QueueService;

public class SingleProducerConsumer {
	
	public static void main(String[] args) throws InterruptedException {
		String serverUri = "amqps://admin:WMKHLLRAANCIOMLS@bluemix-sandbox-dal-9-portal.4.dblayer.com:23928/bmix_lon_yp_64927a10_b72e_4e35_a4a9_d7ea2b5e0a4f";
		String queueName = "Testqueue1";

		QueueService queueService =  new QueueService(serverUri);
		queueService.deleteQueue(queueName);
		queueService.createQueue(queueName);
		
		MessageProducer producer = new MessageProducer(queueName, serverUri);
		Thread producerThread = new Thread(producer);
		producerThread.start();
		
		Thread.sleep(3000);
		
		MessageConsumer consumer = new MessageConsumer(queueName, serverUri);
		Thread consumerThread = new Thread(consumer);
		consumerThread.start();
	}
}
