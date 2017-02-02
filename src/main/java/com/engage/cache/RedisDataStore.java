
package com.engage.cache;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.exceptions.JedisException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

/**
 * RedisDataStore is a client to interact with Redis DB. It mainly,<br/>
 * - Validates the config props given for establishing the connection<br/>
 * - Creates a Jedis connections pool to connect to Redis and holds onto the
 * pool reference for further transactions<br/>
 * - Provides additional functionalities like, returning dafaultValues for cases
 * where no values are returned from the table.
 * 
 * @author saurabh.nigam@vizury.com
 */
public class RedisDataStore {

  /**
   * pool of connections.
   */
  private JedisPool jedisPool;

  /** Configuration properties for Jedis pool. */
  private JedisPoolConfig poolConfig;

  /**
   * Creates a RedisDataStore instance.
   * 
   * @param redisProperties
   *          an object of ConfigProperties containing all the properties for
   *          Redis
   */
  public RedisDataStore(String redisProperties) {
    poolConfig = new JedisPoolConfig();
    poolConfig.setMaxTotal(50);
    poolConfig.setTestOnBorrow(true);
    poolConfig.setTestOnReturn(true);
    poolConfig.setLifo(false);
    jedisPool = new JedisPool(poolConfig, "localhost", 6379, 50);
    // jedisPool = new JedisPool(poolConfig,
    // StringUtils.isBlank(redisProperties.getString("hostname")) ? "localhost"
    // : redisProperties.getString("hostname"), IntegerUtils.parseInt(
    // redisProperties.getString("port"), 6379), IntegerUtils.parseInt(
    // redisProperties.getString("timeout"), 50));
  }

  /**
   * @return Configuration properties used for Jedis pool configuration.
   */
  public JedisPoolConfig getPoolConfig() {
    return this.poolConfig;
  }

  /**
   * {@inheritDoc}.
   */
  public void init() {
  }

  /**
   * {@inheritDoc}.
   * 
   * @param key.
   * @return the value stored in Redis for this key.
   */
  public String getString(String key) {
    if (key == null) {
      return null;
    } else {
      Jedis jedis = null;
      String value = null;
      try {
        jedis = jedisPool.getResource();
        value = jedis.get(key);
      } catch (JedisException ex) {
        System.out.println(ex.getMessage());
      } finally {
        if (jedis != null) {
          jedis.close();
        }
      }
      return value;
    }
  }

  /**
   * {@inheritDoc}.
   * 
   * @param key.
   * @param defaultValue.
   * @return returns the value stored in Redis for this key. In case it is null,
   *         then defaultValue.
   */
  public String getString(String key, String defaultValue) {
    String value = null;
    return (value = getString(key)) == null ? defaultValue : value;
  }

  /**
   * {@inheritDoc}.
   * 
   * @param keyList.
   * @return returns bulk response as per the list of keys.
   */
  public Map<String, String> getBulkString(List<String> keyList) {
    Map<String, String> responseMap = new HashMap<String, String>();
    if (keyList == null || keyList.isEmpty()) {
      return responseMap;
    } else {
      Jedis jedis = null;
      List<String> responseList = null;
      try {
        jedis = jedisPool.getResource();
        responseList = jedis.mget(keyList.toArray(new String[keyList.size()]));
      } catch (JedisException ex) {
        System.out.println(ex.getMessage());
        return responseMap;
      } finally {
        if (jedis != null) {
          jedis.close();
        }
      }
      int index = 0;
      for (String key : keyList) {
        responseMap.put(key, responseList.get(index));
        index++;
      }
      return responseMap;
    }
  }

  /**
   * {@inheritDoc}.
   * 
   * @param keyDefaultValueMap.
   * @return returns bulk response as per the list of keys. In case some or all
   *         of them give null response, it is substituted with default values
   *         as per keyDefaultValueMap.
   */
  public Map<String, String> getBulkString(Map<String, String> keyDefaultValueMap) {
    Map<String, String> responseMap = new HashMap<String, String>();
    if (keyDefaultValueMap == null || keyDefaultValueMap.isEmpty()) {
      return responseMap;
    } else {
      List<String> keyList = new ArrayList<String>();
      for (Entry<String, String> entry : keyDefaultValueMap.entrySet()) {
        keyList.add(entry.getKey());
      }
      responseMap = getBulkString(keyList);
      for (Entry<String, String> entry : responseMap.entrySet()) {
        if (entry.getValue() == null) {
          entry.setValue(keyDefaultValueMap.get(entry.getKey()));
        }
      }
      return responseMap;
    }
  }

  /**
   * {@inheritDoc}.
   */
  public void shutDown() {
    if (jedisPool != null) {
      jedisPool.close();
    }
  }

  /**
   * setStringWithExpiry TODO : Improve this comment.
   * 
   * @param keyValueMap.
   * @param expiry.
   */
  public void setStringWithExpiry(Map<String, Object> keyValueMap, int expiry) {
    if (keyValueMap != null && !keyValueMap.isEmpty()) {
      Jedis jedis = null;
      try {
        jedis = jedisPool.getResource();
        Pipeline pipeline = jedis.pipelined();
        for (Entry<String, Object> entry : keyValueMap.entrySet()) {
          pipeline.setex(entry.getKey(), expiry, (String) entry.getValue());
        }
        pipeline.sync();
      } catch (JedisException ex) {
        System.out.println(ex.getMessage());
      } finally {
        if (jedis != null) {
          jedis.close();
        }
      }
    }
  }

  public void putData(String key, String value) {
    Jedis jedis = null;
    try {
      jedis = jedisPool.getResource();
      jedis.set(key, value);
    } catch (JedisException ex) {
      System.out.println(ex.getMessage());
    } finally {
      if (jedis != null) {
        jedis.close();
      }
    }
  }

  public String getStringFromColumn(String key, String column) {
    throw new UnsupportedOperationException("Not supported yet."); // To change
                                                                   // body of
                                                                   // generated
                                                                   // methods,
                                                                   // choose
                                                                   // Tools |
                                                                   // Templates.
  }

  public String getStringFromTable(String key, String table, String column) {
    throw new UnsupportedOperationException("Not supported yet."); // To change
                                                                   // body of
                                                                   // generated
                                                                   // methods,
                                                                   // choose
                                                                   // Tools |
                                                                   // Templates.
  }

  public String getStringFromDb(String key, String db, String table, String column) {
    throw new UnsupportedOperationException("Not supported yet."); // To change
                                                                   // body of
                                                                   // generated
                                                                   // methods,
                                                                   // choose
                                                                   // Tools |
                                                                   // Templates.
  }

  public String getIntFromColumn(String key, String column) {
    throw new UnsupportedOperationException("Not supported yet."); // To change
                                                                   // body of
                                                                   // generated
                                                                   // methods,
                                                                   // choose
                                                                   // Tools |
                                                                   // Templates.
  }

  public String getIntFromTable(String key, String table, String column) {
    throw new UnsupportedOperationException("Not supported yet."); // To change
                                                                   // body of
                                                                   // generated
                                                                   // methods,
                                                                   // choose
                                                                   // Tools |
                                                                   // Templates.
  }

  public String getIntFromDb(String key, String db, String table, String column) {
    throw new UnsupportedOperationException("Not supported yet."); // To change
                                                                   // body of
                                                                   // generated
                                                                   // methods,
                                                                   // choose
                                                                   // Tools |
                                                                   // Templates.
  }

  public String getDoubleFromColumn(String key, String column) {
    throw new UnsupportedOperationException("Not supported yet."); // To change
                                                                   // body of
                                                                   // generated
                                                                   // methods,
                                                                   // choose
                                                                   // Tools |
                                                                   // Templates.
  }

  public String getDoubleFromTable(String key, String table, String column) {
    throw new UnsupportedOperationException("Not supported yet."); // To change
                                                                   // body of
                                                                   // generated
                                                                   // methods,
                                                                   // choose
                                                                   // Tools |
                                                                   // Templates.
  }

  public String getDoubleFromDb(String key, String db, String table, String column) {
    throw new UnsupportedOperationException("Not supported yet."); // To change
                                                                   // body of
                                                                   // generated
                                                                   // methods,
                                                                   // choose
                                                                   // Tools |
                                                                   // Templates.
  }

  public String getFloatFromColumn(String key, String column) {
    throw new UnsupportedOperationException("Not supported yet."); // To change
                                                                   // body of
                                                                   // generated
                                                                   // methods,
                                                                   // choose
                                                                   // Tools |
                                                                   // Templates.
  }

  public String getFloatFromTable(String key, String table, String column) {
    throw new UnsupportedOperationException("Not supported yet."); // To change
                                                                   // body of
                                                                   // generated
                                                                   // methods,
                                                                   // choose
                                                                   // Tools |
                                                                   // Templates.
  }

  public String getFloatFromDb(String key, String db, String table, String column) {
    throw new UnsupportedOperationException("Not supported yet."); // To change
                                                                   // body of
                                                                   // generated
                                                                   // methods,
                                                                   // choose
                                                                   // Tools |
                                                                   // Templates.
  }

  public Set<String> getColumns(String key, String set) {
    throw new UnsupportedOperationException("Not supported yet."); // To change
                                                                   // body of
                                                                   // generated
                                                                   // methods,
                                                                   // choose
                                                                   // Tools |
                                                                   // Templates.
  }

  public Set<String> getActiveColumns(String key, String set) {
    throw new UnsupportedOperationException("Not supported yet."); // To change
                                                                   // body of
                                                                   // generated
                                                                   // methods,
                                                                   // choose
                                                                   // Tools |
                                                                   // Templates.
  }

  public Boolean setElement(String set, String bin, String key, String value, int generation,
      int ttl) {
    // TODO Auto-generated method stub
    return null;
  }
}
