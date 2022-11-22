package com.hmdp.utils;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.ZoneOffset;

/**
 * 全局id自动生成
 */
@Component
public class RedisIdWorker {
    //开始时间戳 2022.1.1.0.0.0
    private static final long BEGIN_TIMESTAMP = 1640995200L;
    //序列号位数
    private static final int COUNT_BITS = 32;

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    public long nextId(String keyPrefix) {
        //生成时间戳
        LocalDateTime now = LocalDateTime.now();
        long nowSecond = now.toEpochSecond(ZoneOffset.UTC);
        long timestatmp = nowSecond - BEGIN_TIMESTAMP;
        //生成序列号
        String data = now.format(DateTimeFormatter.ofPattern("yyyy:MM:dd"));
        //自增长
        long count = stringRedisTemplate.opsForValue().increment("icr:" + keyPrefix + ":" + data);

        return timestatmp << COUNT_BITS | count;
    }
}
