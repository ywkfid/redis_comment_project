package com.hmdp.service.impl;

import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONUtil;
import com.hmdp.dto.Result;
import com.hmdp.entity.ShopType;
import com.hmdp.mapper.ShopTypeMapper;
import com.hmdp.service.IShopTypeService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.List;

@Service
public class ShopTypeServiceImpl extends ServiceImpl<ShopTypeMapper, ShopType> implements IShopTypeService {
    @Resource
    private StringRedisTemplate stringRedisTemplate;

    /**
     * 查询商店分类信息
     * @return
     */
    @Override
    public Result queryTypeList() {
        String key = "cache:shopType";
        //redis查看是否有商店种类缓存
        String shopTypeJson = stringRedisTemplate.opsForValue().get(key);
        //有的话就转换成对象数据返回
        if (StrUtil.isNotBlank(shopTypeJson)) {
            List<ShopType> shopTypes = JSONUtil.toList(shopTypeJson, ShopType.class);
            return Result.ok(shopTypes);
        }
        //没有的话就查找数据库里是否有
        List<ShopType> shopTypes = query().orderByAsc("sort").list();
        if (shopTypes == null) {
            return Result.fail("店铺种类不存在!");
        }
        //把数据库查到的存入redis缓存
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(shopTypes));
        //返回
        return Result.ok(shopTypes);
    }
}
