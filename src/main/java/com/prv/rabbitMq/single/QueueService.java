package com.prv.rabbitMq.single;

import java.io.IOException;
import java.net.URISyntaxException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.TimeoutException;

import com.rabbitmq.client.AMQP.Queue.DeclareOk;
import com.rabbitmq.client.AMQP.Queue.DeleteOk;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

public class QueueService {

	private String serverUri;

	public QueueService(String serverUri) {
		super();
		this.serverUri = serverUri;
	}
	
	public void createQueue(String queueName){
		Connection connection = null;
		Channel channel = null;

		try {
			ConnectionFactory factory = new ConnectionFactory();
			factory.setUri(this.serverUri);
			
			// The connection abstracts the socket connection, and takes care of
			// protocol version negotiation and authentication etc..
			connection = factory.newConnection();
			channel = connection.createChannel();

			// Declaring a queue is idempotent - it will only be created if it
			// doesn't exist already
			DeclareOk queueDeclare = channel.queueDeclare(queueName, false, false, false, null);

			System.out.println("Created queue "+ queueName + ":" + queueDeclare);
		} catch (KeyManagementException | NoSuchAlgorithmException | URISyntaxException | IOException
				| TimeoutException e) {
			System.out.println("Exception: " + e.getMessage());
		} finally {
			closeResources(connection, channel);
		}
	}
	
	public void deleteQueue(String queueName){
		Connection connection = null;
		Channel channel = null;

		try {
			ConnectionFactory factory = new ConnectionFactory();
			factory.setUri(this.serverUri);
			
			// The connection abstracts the socket connection, and takes care of
			// protocol version negotiation and authentication etc..
			connection = factory.newConnection();
			channel = connection.createChannel();

			DeleteOk queueDelete = channel.queueDelete(queueName, false, false);

			System.out.println("Deleted queue "+ queueName + ":" + queueDelete);
		} catch (KeyManagementException | NoSuchAlgorithmException | URISyntaxException | IOException
				| TimeoutException e) {
			System.out.println("Exception: " + e.getMessage());
		} finally {
			closeResources(connection, channel);
		}
	}
	
	private void closeResources(Connection connection, Channel channel) {
		System.out.println("Closing consumer channel, connection.");
		if (channel != null) {
			try {
				channel.close();
			} catch (IOException | TimeoutException e) {
			}
		}

		if (connection != null) {
			try {
				connection.close();
			} catch (IOException e) {

			}
		}
	}
}
