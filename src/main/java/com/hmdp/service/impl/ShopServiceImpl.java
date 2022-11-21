package com.hmdp.service.impl;

import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.hmdp.dto.Result;
import com.hmdp.entity.Shop;
import com.hmdp.mapper.ShopMapper;
import com.hmdp.service.IShopService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.CacheClient;
import com.hmdp.utils.RedisData;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Resource;

import java.time.LocalDateTime;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static com.hmdp.utils.RedisConstants.*;

@Service
public class ShopServiceImpl extends ServiceImpl<ShopMapper, Shop> implements IShopService {
    @Resource
    private StringRedisTemplate stringRedisTemplate;
    @Resource
    private CacheClient cacheClient;

    /**
     * 根据id查询店铺信息
     * @param id
     * @return
     */
    @Override
    public Result queryById(Long id) {
        //缓存穿透
        //Shop shop = queryWithPassThrough(id);
        //Shop shop = cacheClient.queryWithPassThrough(CACHE_SHOP_KEY, id, Shop.class, this::getById, CACHE_SHOP_TTL, TimeUnit.MINUTES);
        //互斥锁解决缓存击穿
        //Shop shop = queryWithMutex(id);
        //逻辑过期解决缓存击穿
        //Shop shop = queryWithLogicalExpire(id);
        Shop shop = cacheClient
                .queryWithPassThrough(CACHE_SHOP_KEY, id, Shop.class, this::getById, CACHE_SHOP_TTL, TimeUnit.MINUTES);
        if (shop == null) {
            return Result.fail("店铺不存在");
        }
        //返回数据
        return Result.ok(shop);
    }

//    private static final ExecutorService CACHE_REBUILD_EXECUTOR = Executors.newFixedThreadPool(10);
    /**
     * 逻辑删除解决缓存击穿
     * @param id
     * @return
     */
//    public Shop queryWithLogicalExpire(Long id) {
//        String key = CACHE_SHOP_KEY + id;
//        //从redis查询商铺缓存
//        String shopJson = stringRedisTemplate.opsForValue().get(key);
//        //判断是否存在
//        if (StrUtil.isBlank(shopJson)) {
//            //空的话则返回空
//            return null;
//        }
//        //命中则先把json转为对象
//        RedisData redisData = JSONUtil.toBean(shopJson, RedisData.class);
//        Shop shop = JSONUtil.toBean((JSONObject) redisData.getData(), Shop.class);
//        LocalDateTime expireTime = redisData.getExpireTime();
//        //判断是否过期
//        if (expireTime.isAfter(LocalDateTime.now())) {
//            //未过期直接返回店铺信息
//            return shop;
//        }
//        //过期就重建缓存并获取互斥锁
//        String lockKey = LOCK_SHOP_KEY + id;
//        boolean isLock = tryLock(lockKey);
//        if (isLock) {
//            //这里应该再做一次判断看缓存是否过期，做双重check，如果存在则无需重建直接返回
//            //以免多开启线程，此处暂时省略
//            //开启独立线程，实现缓存重组
//            CACHE_REBUILD_EXECUTOR.submit(() -> {
//                try {
//                    this.saveShop2Redis(id, 20L);
//                } catch (Exception e) {
//                    throw new RuntimeException(e);
//                } finally {
//                    //释放锁
//                    unlock(lockKey);
//                }
//            });
//        }
//        //返回数据
//        return shop;
//    }

    /**
     * 用互斥锁解决缓存击穿
     * @param id
     * @return
     */
//    public Shop queryWithMutex(Long id) {
//        String key = CACHE_SHOP_KEY + id;
//        //从redis查询商铺缓存
//        String shopJson = stringRedisTemplate.opsForValue().get(key);
//        //判断是否存在
//        if (StrUtil.isNotBlank(shopJson)) {
//            //存在则返回
//            Shop shop = JSONUtil.toBean(shopJson, Shop.class);
//            return shop;
//        }
//        //不存在则判断命中的是否是空值，是空值就返回错误信息
//        if ("".equals(shopJson)) {
//            return null;
//        }
//        //实现缓存重建
//        //获取互斥锁，判断是否获取成功，失败就失眠然后重试，成功就根据id查询数据库
//        String lockKey =  LOCK_SHOP_KEY + id;
//        Shop shop = null;
//        try {
//            boolean isLock = tryLock(lockKey);
//            if (!isLock) {
//                //已经上锁的话就休眠然后重试
//                Thread.sleep(50);
//                return queryWithMutex(id); //递归非常不好
//            }
//            //根据id查询数据库
//            shop = getById(id);
//            //不存在返回错误
//            if (shop == null) {
//                //防止缓存穿透还需要把空值写入redis
//                stringRedisTemplate.opsForValue().set(key, "", CACHE_NULL_TTL, TimeUnit.MINUTES);
//                return null;
//            }
//            //存在，写入redis并添加过期时间
//            stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(shop), CACHE_SHOP_TTL, TimeUnit.MINUTES);
//        } catch (InterruptedException e) {
//            throw new RuntimeException(e);
//        } finally {
//            //释放锁
//            unlock(lockKey); //加上trycatch避免出现异常没有及时释放锁
//        }
//        //返回数据
//        return shop;
//    }

    /**
     * 防止缓存穿透
     * @param id
     * @return
     */
//    public Shop queryWithPassThrough(Long id) {
//        String key = CACHE_SHOP_KEY + id;
//        //从redis查询商铺缓存
//        String shopJson = stringRedisTemplate.opsForValue().get(key);
//        //判断是否存在
//        if (StrUtil.isNotBlank(shopJson)) {
//            //存在则返回
//            Shop shop = JSONUtil.toBean(shopJson, Shop.class);
//            return shop;
//        }
//        //不存在则判断命中的是否是空值，是空值就返回错误信息
//        if ("".equals(shopJson)) {
//            return null;
//        }
//        //根据id查询数据库
//        Shop shop = getById(id);
//        //不存在返回错误
//        if (shop == null) {
//            //防止缓存穿透还需要把空值写入redis
//            stringRedisTemplate.opsForValue().set(key, "", CACHE_NULL_TTL, TimeUnit.MINUTES);
//            return null;
//        }
//        //存在，写入redis并添加过期时间
//        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(shop), CACHE_SHOP_TTL, TimeUnit.MINUTES);
//        //返回数据
//        return shop;
//    }

    /**
     * 互斥锁
     * @param key
     * @return
     */
//    private boolean tryLock(String key) {
//        Boolean flag = stringRedisTemplate.opsForValue().setIfAbsent(key, "1", 30L, TimeUnit.SECONDS);
//        return BooleanUtil.isTrue(flag);
//    }
//    private void unlock(String key) {
//        stringRedisTemplate.delete(key);
//    }

    /**
     * 提前把热点key加上逻辑过期时间存入redis
     * @param id
     */
//    public void saveShop2Redis(Long id, Long expireSeconds) throws InterruptedException {
//        //查询店铺数据
//        Shop shop = getById(id);
//        Thread.sleep(200);
//        //封装逻辑过期时间
//        RedisData redisData = new RedisData();
//        redisData.setData(shop);
//        redisData.setExpireTime(LocalDateTime.now().plusSeconds(expireSeconds));
//        //写入redis
//        stringRedisTemplate.opsForValue().set(CACHE_SHOP_KEY + id, JSONUtil.toJsonStr(redisData));
//    }
    /**
     * 更新店铺信息
     * @param shop
     * @return
     */
    @Override
    @Transactional //并不能对redis缓存进行回滚
    public Result update(Shop shop) {
        Long id = shop.getId();
        if (id == null) {
            return Result.fail("店铺id不能为空");
        }
        //更新数据库
        updateById(shop);
        //删除缓存
        stringRedisTemplate.delete(CACHE_SHOP_KEY + id);

        return Result.ok();
    }
}
