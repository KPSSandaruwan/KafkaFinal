package com.suraja.kafka_consumer;

import com.suraja.kafka.kafka_consumer.config.Message;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.KafkaListener;

import javax.ws.rs.client.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

@SpringBootApplication
public class KafkaConsumerApplication {

    public static String data;
    public static String data2;
    static JSONParser parser = new JSONParser();


    @KafkaListener(groupId = "group-1", topics = "new-feedback2", containerFactory = "kafkaListenerContainerFactory")
    public String getMsgFromTopic(String dat) throws ParseException {
        data = dat;
        JSONObject jsonObject = (JSONObject) parser.parse(data);
        data2 = jsonObject.toJSONString();
        System.out.println(data2);
        getFeed();
        return data2;
    }

    public static void getFeed() {
        Client client = ClientBuilder.newClient();

        WebTarget resorce = client.target("http://127.0.0.1:5000/store");
        Message msg = new Message(data2);
        msg.setMsg(data2);
//        System.out.println(data2);
        Invocation.Builder request = resorce.request(MediaType.APPLICATION_JSON);
        Response response = request.post(Entity.entity(data2, MediaType.APPLICATION_JSON));

        System.out.println(data2);
        System.out.println(response.getStatus());
        System.out.println(response.readEntity(String.class));
    }

    public static void main(String[] args) {
        SpringApplication.run(KafkaConsumerApplication.class, args);
    }

}
