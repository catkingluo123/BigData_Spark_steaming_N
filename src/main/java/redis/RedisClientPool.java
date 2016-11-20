package redis;

/**
 * Created by lenovo on 2016/11/16.
 */
import org.apache.log4j.Logger;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import util.Config;

import java.io.Serializable;
import java.util.Properties;

public class RedisClientPool implements Serializable {

    private static final Logger logger = Logger.getLogger(RedisClientPool.class);

    private static final RedisClientPool redisClientPool = new RedisClientPool();

    private JedisPool jedisPool;

    public RedisClientPool(){

        JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
        Properties properties = Config.getConfig("redis.properties");
        jedisPoolConfig.setMaxTotal(Integer.parseInt(properties.getProperty("redis.pool.maxTotal")));
        jedisPoolConfig.setMaxIdle(Integer.parseInt(properties.getProperty("redis.pool.maxIdle")));
        jedisPoolConfig.setMaxWaitMillis(Integer.parseInt(properties.getProperty("redis.pool.maxWait")));
        jedisPoolConfig.setTestOnBorrow(Boolean.valueOf(properties.getProperty("redis.pool.testOnBorrow")));
        jedisPoolConfig.setTestOnReturn(Boolean.valueOf(properties.getProperty("redis.pool.testOnReturn")));
        jedisPool = new JedisPool(jedisPoolConfig ,properties.getProperty("redis.ip") ,
                Integer.parseInt(properties.getProperty("redis.port")) ,3000,
                properties.getProperty("redis.password"));

        Runtime.getRuntime().addShutdownHook(new Thread(){
            @Override
            public void run() {
                logger.warn("redis pool destroy");
                if(null!=jedisPool){
                    jedisPool.destroy();
                    jedisPool=null;
                }
            }
        });
    }

    public static RedisClientPool getInstance(){
        return redisClientPool;
    }

    public JedisPool getPool(){
        logger.warn("get a redis pool");
        return jedisPool;
    }
}
