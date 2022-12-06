package org.stefan;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.MessageProperties;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.Scanner;

public class Main {
    private static final String TASK_QUEUE_NAME = "smart_metering_device_simulator";
    private static final Logger LOGGER = LoggerFactory.getLogger(Main.class);

    public static void main(String[] argv) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");

        Scanner scannerId = new Scanner(System.in);
        System.out.println("Enter device id: ");
        Long deviceId = Long.parseLong(scannerId.nextLine());

        try (Connection connection = factory.newConnection();
             Channel channel = connection.createChannel()) {
            boolean durable = false; // durable - RabbitMQ will never lose the queue if a crash occurs
            boolean exclusive = false; // exclusive - if queue only will be used by one connection
            boolean autoDelete = false; // autoDelete - queue is deleted when last consumer unsubscribe
            channel.queueDeclare(TASK_QUEUE_NAME, durable, exclusive, autoDelete, null);

            File file = new File("D:\\rabbitmq-producer\\sensor.csv");
            Scanner scannerFile = new Scanner(file);

            while (scannerFile.hasNextLine()) {
                String value = scannerFile.nextLine();
                JSONObject object = new JSONObject();

                object.put("timestamp", Timestamp.from(Instant.now()));
                object.put("deviceId", deviceId.toString());
                object.put("energyConsumption", value);

                channel.basicPublish("", TASK_QUEUE_NAME, null, object.toString().getBytes(StandardCharsets.UTF_8));
                System.out.println(" [x] Sent " + object);

                Thread.sleep(1000);
            }
        } catch (Exception e) {
            LOGGER.error("Exception: {}; exception cause: {}", e.getMessage(), e.getCause());
        }
    }
}