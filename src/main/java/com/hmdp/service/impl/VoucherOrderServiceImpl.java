package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import com.hmdp.dto.Result;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.service.ISeckillVoucherService;
import com.hmdp.service.IVoucherOrderService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.RedisIdWorker;
import com.hmdp.utils.UserHolder;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.aop.framework.AopContext;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.connection.stream.*;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Slf4j
@Service
public class VoucherOrderServiceImpl extends ServiceImpl<VoucherOrderMapper, VoucherOrder> implements IVoucherOrderService {
    @Resource
    private ISeckillVoucherService seckillVoucherService;
    @Resource
    private RedisIdWorker redisIdWorker;
    @Resource
    private StringRedisTemplate stringRedisTemplate;
    @Resource
    private RedissonClient redissonClient;

    private static final DefaultRedisScript<Long> SECKILL_SCRIPT;
    static {
        SECKILL_SCRIPT = new DefaultRedisScript<>();
        SECKILL_SCRIPT.setLocation(new ClassPathResource("seckill.lua"));
        SECKILL_SCRIPT.setResultType(Long.class);
    }


    //线程池 堵塞队列
    private static final ExecutorService SECKILL_ORDER_EXECUTOR = Executors.newSingleThreadExecutor();
    @PostConstruct
    private void init() {
        SECKILL_ORDER_EXECUTOR.submit(new VoucherOrderHandler());
    }

    private class VoucherOrderHandler implements Runnable {
        String queueName = "stream.orders";

        @Override
        public void run() {
            while (true) {
                try {
                    //获取消息队列中的订单信息 XREADGROUP GROUP g1 c1 COUNT 1 BLOCK 2000 stream.orders >
                    List<MapRecord<String, Object, Object>> list = stringRedisTemplate.opsForStream().read(
                            Consumer.from("g1", "c1"),
                            StreamReadOptions.empty().count(1).block(Duration.ofSeconds(2)),
                            StreamOffset.create(queueName, ReadOffset.lastConsumed())
                    );
                    //判断消息获取是否成功
                    if (list == null || list.isEmpty()) {
                        //如果获取失败，说明没有消息，继续下一次循环
                        continue;
                    }
                    //从list里解析消息
                    MapRecord<String, Object, Object> record = list.get(0);
                    Map<Object, Object> values = record.getValue();
                    VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(values, new VoucherOrder(), true);
                    //如果获取成功，则可以下单
                    handleVoucherOrder(voucherOrder);
                    //ACK确认 stream.orders g1 id
                    stringRedisTemplate.opsForStream().acknowledge(queueName, "g1", record.getId());
                } catch (Exception e) {
                    log.error("处理订单异常", e);
                    handlePendingList();
                }
            }
        }

        private void handlePendingList() {
            while (true) {
                try {
                    //获取pendinglist的订单信息 XREADGROUP GROUP g1 c1 COUNT 1 stream.orders >
                    List<MapRecord<String, Object, Object>> list = stringRedisTemplate.opsForStream().read(
                            Consumer.from("g1", "c1"),
                            StreamReadOptions.empty().count(1),
                            StreamOffset.create(queueName, ReadOffset.from("0"))
                    );
                    //判断消息获取是否成功
                    if (list == null || list.isEmpty()) {
                        //如果获取失败，说明pendinglist没有消息，继续下一次循环
                        break;
                    }
                    //从list里解析消息
                    MapRecord<String, Object, Object> record = list.get(0);
                    Map<Object, Object> values = record.getValue();
                    VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(values, new VoucherOrder(), true);
                    //如果获取成功，则可以下单
                    handleVoucherOrder(voucherOrder);
                    //ACK确认 stream.orders g1 id
                    stringRedisTemplate.opsForStream().acknowledge(queueName, "g1", record.getId());
                } catch (Exception e) {
                    log.error("处理pendinglist异常", e);
                }
            }
        }
    }
    /**
     * 阻塞队列实现消息队列
     */
/*
    private BlockingQueue<VoucherOrder> orderTasks = new ArrayBlockingQueue<>(1024 * 1024);
    private class VoucherOrderHandler implements Runnable {
        @Override
        public void run() {
            while (true) {
                try {
                    //获取队列中的订单信息
                    VoucherOrder voucherOrder = orderTasks.take();
                    //创建订单
                    handleVoucherOrder(voucherOrder);
                } catch (Exception e) {
                    log.error("处理订单异常", e);
                }
            }
        }
    }*/


    private void handleVoucherOrder(VoucherOrder voucherOrder) {
        Long userId = voucherOrder.getId();
        //用redisson获取锁
        RLock lock = redissonClient.getLock("lock:order:" + userId);

        //boolean isLock = lock.tryLock(1200);
        boolean isLock = lock.tryLock();

        if (!isLock) {
            //获取锁失败， 返回错误或重试
            log.error("不允许重复下单");
            return;
        }
        //获取事务代理对象
        try {
            //这里获取不到代理对象 这里是子线程
            proxy.createVoucherOrder(voucherOrder);
        } finally {
            lock.unlock();
        }
    }

    private IVoucherOrderService proxy;

    @Override
    public Result seckillVoucher(Long voucherId) {
        Long userId = UserHolder.getUser().getId();
        // 订单id
        long orderId = redisIdWorker.nextId("order");
        // 执行lua脚本
        Long result = stringRedisTemplate.execute(
                SECKILL_SCRIPT,
                Collections.emptyList(),
                voucherId.toString(), userId.toString(), String.valueOf(orderId)
        );

        int r = result.intValue();
        // 判断结果是否为0
        if (r != 0) {
            // 不为0,代表没有购买资格
            return Result.fail(r == 1 ? "库存不足" : "不能重复下单");
        }

        //获取代理对象
        proxy = (IVoucherOrderService) AopContext.currentProxy();

        // 3.返回订单id
        return Result.ok(orderId);
    }

//    @Override
//    public Result seckillVoucher(Long voucherId) {
//        Long userId = UserHolder.getUser().getId();
//        // 执行lua脚本
//        Long result = stringRedisTemplate.execute(
//                SECKILL_SCRIPT,
//                Collections.emptyList(),
//                voucherId.toString(), userId.toString()
//        );
//        int r = result.intValue();
//        // 判断结果是否为0
//        if (r != 0) {
//            // 不为0,代表没有购买资格
//            return Result.fail(r == 1 ? "库存不足" : "不能重复下单");
//        }
//        // 为0,有购买资格，把下单信息保存到阻塞队列
//        VoucherOrder voucherOrder = new VoucherOrder();
//        // 订单id
//        long orderId = redisIdWorker.nextId("order");
//        voucherOrder.setId(orderId);
//        // 用户id
//        voucherOrder.setUserId(userId);
//        // 代金券id
//        voucherOrder.setVoucherId(voucherId);
//        // 放入阻塞队列
//        orderTasks.add(voucherOrder);
//        //获取代理对象
//        proxy = (IVoucherOrderService) AopContext.currentProxy();
//
//        // 3.返回订单id
//        return Result.ok(orderId);
//    }


//    @Override
//    public Result seckillVoucher(Long voucherId) {
//        //查询优惠卷
//        SeckillVoucher voucher = seckillVoucherService.getById(voucherId);
//        //判断秒杀是否开始或结束
//        if (voucher.getBeginTime().isAfter(LocalDateTime.now())) {
//            return Result.fail("秒杀尚未开始");
//        }
//        if (voucher.getEndTime().isBefore(LocalDateTime.now())) {
//            return Result.fail("秒杀已经结束");
//        }
//        if (voucher.getStock() < 1) {
//            return Result.fail("库存不足");
//        }
//        //一人一单
//        Long userId = UserHolder.getUser().getId();
//
//        //intern()保证值一样 直接的话事务会失效相当于this. 没有事务功能
//        //不用synchronized了 因为防止不了分布式的情况 因为每个jvm都有自己的锁监控
//        //synchronized (userId.toString().intern())
//        //SimpleRedisLock lock = new SimpleRedisLock("order:" + userId, stringRedisTemplate);
//        //用redisson获取锁
//        RLock lock = redissonClient.getLock("lock:order:" + userId);
//
//        //boolean isLock = lock.tryLock(1200);
//        boolean isLock = lock.tryLock();
//
//        if (!isLock) {
//            return Result.fail("不允许重复下单");
//        }
//        //获取事务代理对象
//        try {
//            IVoucherOrderService proxy = (IVoucherOrderService) AopContext.currentProxy();
//            return proxy.createVoucherOrder(voucherId);
//        } finally {
//            lock.unlock();
//        }
//    }


    @Transactional
    public void createVoucherOrder(VoucherOrder voucherOrder) {
        //因为是异步 不能用threadlocal获取
        //Long userId = UserHolder.getUser().getId();
        Long userId = voucherOrder.getUserId();

        int count = query().eq("user_id", userId).eq("voucher_id", voucherOrder.getVoucherId()).count();
        //已经抢过了
        if (count > 0) {
            //不太可能重复 不需要返回 以防万一记录日志
            log.error("用户已经购买过一次");
            return;
        }

        //更新时查看一下是否库存是否是和之前查到的一样这样失败率太高，判断库存是否大于0就行
        boolean success = seckillVoucherService.update()
                .setSql("stock = stock - 1")
                .eq("voucher_id", voucherOrder.getVoucherId()).gt("stock", 0)
                .update();

        if (!success) {
            log.error("库存不足");
            return;
        }

        //创建订单
//        VoucherOrder voucherOrder = new VoucherOrder();
//        long orderId = redisIdWorker.nextId("order");
//        voucherOrder.setId(orderId);
//        voucherOrder.setUserId(userId);
//        voucherOrder.setVoucherId(voucherOrder);
        save(voucherOrder);
//        //返回订单id
//        return Result.ok(voucherOrder.get);
    }

}
