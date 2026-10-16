package org.redisson.spring.data.connection;

import org.junit.jupiter.api.Test;
import org.redisson.RedisDockerTest;
import org.redisson.Redisson;
import org.redisson.api.RedissonClient;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.RedisSentinelConnection;
import org.springframework.data.redis.connection.RedisServer;
import org.springframework.data.redis.connection.RedisStringCommands.SetOption;
import org.springframework.data.redis.core.types.Expiration;

import java.time.Instant;
import java.util.Collection;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;

public class RedissonSentinelConnectionTest extends RedisDockerTest {

    void withSentinel(Consumer<RedisSentinelConnection> callback) throws InterruptedException {
        withSentinel((containers, c) -> {
            RedissonClient redisson = Redisson.create(c);

            RedissonConnectionFactory factory = new RedissonConnectionFactory(redisson);
            RedisSentinelConnection connection = factory.getSentinelConnection();
            callback.accept(connection);
        }, 2);
    }

    @Test
    public void testWriteCommandsWithReplica() throws InterruptedException {
        withSentinel((containers, c) -> {
            RedissonClient redisson = Redisson.create(c);
            try {
                RedissonConnectionFactory factory = new RedissonConnectionFactory(redisson);
                RedisConnection connection = factory.getConnection();

                byte[] key = "key".getBytes();
                connection.stringCommands().set(key, "value1".getBytes());
                assertThat(connection.stringCommands().setGet(key, "value2".getBytes(), Expiration.persistent(), SetOption.upsert()))
                        .isEqualTo("value1".getBytes());

                byte[] hash = "hash".getBytes();
                connection.hashCommands().hSet(hash, "field1".getBytes(), "value1".getBytes());
                long unixTime = Instant.now().plusSeconds(60).getEpochSecond();
                assertThat(connection.hashCommands().hExpireAt(hash, unixTime, "field1".getBytes())).containsExactly(1L);
            } finally {
                redisson.shutdown();
            }
        }, 1);
    }

    @Test
    public void testMasters() throws InterruptedException {
        withSentinel(connection -> {
            Collection<RedisServer> masters = connection.masters();
            assertThat(masters).hasSize(1);
        });
    }
    
    @Test
    public void testSlaves() throws InterruptedException {
        withSentinel(connection -> {
            Collection<RedisServer> masters = connection.masters();
            Collection<RedisServer> slaves = connection.replicas(masters.iterator().next());
            assertThat(slaves).hasSize(2);
        });
    }

    @Test
    public void testRemove() throws InterruptedException {
        withSentinel(connection -> {
            Collection<RedisServer> masters = connection.masters();
            connection.remove(masters.iterator().next());
        });
    }

    @Test
    public void testMonitor() throws InterruptedException {
        withSentinel(connection -> {
            Collection<RedisServer> masters = connection.masters();
            RedisServer master = masters.iterator().next();
            master.setName(master.getName() + ":");
            connection.monitor(master);
        });
    }
    
    @Test
    public void testFailover() throws InterruptedException {
        withSentinel(connection -> {
            Collection<RedisServer> masters = connection.masters();
            connection.failover(masters.iterator().next());

            try {
                Thread.sleep(10000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }

            RedisServer newMaster = connection.masters().iterator().next();
            assertThat(masters.iterator().next().getPort()).isNotEqualTo(newMaster.getPort());
        });
    }
}
