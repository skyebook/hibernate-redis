package net.daum.clix.hibernate.redis.jedis;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.util.ISO8601DateFormat;
import com.fasterxml.jackson.module.afterburner.AfterburnerModule;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;
import net.daum.clix.hibernate.redis.RedisCache;
import org.hibernate.cache.CacheException;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

/**
 * @author jtlee
 * @author 84june
 */
public class JedisCacheImpl implements RedisCache {

    private JedisPool jedisPool;

    private Jedis jedis;

    private String regionName;
    private boolean locked = false;

    public static final ObjectMapper mapper = new ObjectMapper();

    static {
        mapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
        mapper.setDateFormat(new ISO8601DateFormat());
        mapper.registerModule(new AfterburnerModule());
    }

    public JedisCacheImpl(JedisPool jedisPool, String regionName) {
        this.jedisPool = jedisPool;
        this.regionName = regionName;
        this.jedis = jedisPool.getResource();
    }

    @Override
    public Object get(Object key) throws CacheException {
        try {
            Object o = null;

            byte[] k = serializeObject(key.toString());
            byte[] v = jedis.get(k);
            if (v != null && v.length > 0) {
                o = deserializeObject(v);
            }

            return o;
        } catch (IOException ex) {
            throw new CacheException(ex);
        }
    }

    @Override
    public void put(Object key, Object value) throws CacheException {
        try {
            byte[] k = serializeObject(key.toString());
            byte[] v = serializeObject(value);
            
            jedis.set(k, v);
        } catch (IOException ex) {
            throw new CacheException(ex);
        }
    }

    @Override
    public void remove(Object key) throws CacheException {
        try {
            jedis.del(serializeObject(key.toString()));
        } catch (JsonProcessingException ex) {
            throw new CacheException(ex);
        }
    }

    @Override
    public boolean exists(String key) {
        try {
            return jedis.exists(serializeObject(key.toString()));
        } catch (JsonProcessingException ex) {
            Logger.getLogger(JedisCacheImpl.class.getName()).log(Level.SEVERE, null, ex);
            return false;
        }
    }

    @Override
    public int getTimeout() {
        return 0;
    }

    @Override
    public String getRegionName() {
        return this.regionName;
    }

    @Override
    public long getSizeInMemory() {
        return -1;
    }

    @Override
    public long getElementCountInMemory() {
        return -1;
    }

    @Override
    public long getElementCountOnDisk() {
        return -1;
    }

    @Override
    public void destory() {
        jedisPool.returnResource(jedis);
    }

    private byte[] serializeObject(Object obj) throws JsonProcessingException {
        return mapper.writeValueAsBytes(obj);
    }

    private Object deserializeObject(byte[] b) throws IOException {
        return mapper.readValue(b, Object.class);
    }

    public boolean lock(Object key, Long expireMsecs) throws InterruptedException {

        int timeout = getTimeout();
        String lockKey = generateLockKey(key);
        long expires = System.currentTimeMillis() + expireMsecs + 1;
        String expiresStr = String.valueOf(expires);

        while (timeout >= 0) {

            if (jedis.setnx(lockKey, expiresStr) == 1) {
                locked = true;
                return true;
            }

            String currentValueStr = jedis.get(lockKey);
            if (currentValueStr != null && Long.parseLong(currentValueStr) < System.currentTimeMillis()) {
                // lock is expired

                String oldValueStr = jedis.getSet(lockKey, expiresStr);
                if (oldValueStr != null && oldValueStr.equals(currentValueStr)) {
                    // lock acquired
                    locked = true;
                    return true;
                }
            }
            timeout -= 100;
            Thread.sleep(100);
        }
        return false;
    }

    @Override
    public void unlock(Object key) {
        if (locked) {
            jedis.del(generateLockKey(key));
            locked = false;
        }
    }

    private String generateLockKey(Object key) {

        if (null == key) {
            throw new IllegalArgumentException("key must not be null");
        }

        return key.toString() + ".lock";
    }

}
