
package com.engage.kafka;

import com.engage.cache.RedisDataStore;
import com.engage.entity.User;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;

import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;

public class ConsumedDataProcessor implements Runnable {
  private static RedisDataStore redisDataStore = new RedisDataStore("");
  private static ObjectMapper objectMapper = new ObjectMapper();
  private KafkaStream m_stream;
  private int m_threadNumber;

  public ConsumedDataProcessor(KafkaStream a_stream, int a_threadNumber) {
    m_threadNumber = a_threadNumber;
    m_stream = a_stream;
  }

  public void run() {
    ConsumerIterator<byte[], byte[]> it = m_stream.iterator();
    while (it.hasNext()) {
      String analyzeData = new String(it.next().message());
      System.out.println("Thread " + m_threadNumber + ": " + analyzeData);
      updateRedis(analyzeData);
    }
    System.out.println("Shutting down Thread: " + m_threadNumber);
  }

  public void updateRedis(String analyzeData) {
    User newUser = new User(analyzeData);
    System.out.println(newUser);
    String key = newUser.getAccountId() + "_" + newUser.getVizId();
    String oldUserJson = redisDataStore.getString(key);

    User oldUser = null;
    if (oldUserJson != null) {
      try {
        oldUser = objectMapper.readValue(oldUserJson, User.class);
      } catch (JsonParseException e1) {
        e1.printStackTrace();
      } catch (JsonMappingException e1) {
        e1.printStackTrace();
      } catch (IOException e1) {
        e1.printStackTrace();
      }
      oldUser.mergeUserData(newUser);
    } else {
      oldUser = newUser;
    }

    String userJson = null;
    try {
      userJson = objectMapper.writeValueAsString(oldUser);
    } catch (JsonProcessingException e) {
      System.out.println(e.toString());
    }
    System.out.println(key + " : " + userJson);
    redisDataStore.putData(key, userJson);
  }
}
