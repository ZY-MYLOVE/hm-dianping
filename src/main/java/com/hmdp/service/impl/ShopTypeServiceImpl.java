package com.hmdp.service.impl;

import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONUtil;
import com.hmdp.dto.Result;
import com.hmdp.entity.ShopType;
import com.hmdp.mapper.ShopTypeMapper;
import com.hmdp.service.IShopTypeService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.RedisConstants;
import org.springframework.data.redis.core.ListOperations;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ZSetOperations;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.List;

/**
 * <p>
 * 服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class ShopTypeServiceImpl extends ServiceImpl<ShopTypeMapper, ShopType> implements IShopTypeService {
    @Resource
    private StringRedisTemplate stringRedisTemplate;
    @Resource
    private ShopTypeMapper shopTypeMapper;

    @Override
    public Result queryTypeList() {
        String key = RedisConstants.CACHE_SHOP_TYPE; // CACHE_SHOP_TYPE_KEY = "cache:shopType";

        // 1.从 Redis 中查询商铺缓存
        List<String> shopTypeJsonList = stringRedisTemplate.opsForList().range(key, 0, -1);

        // 2.判断 Redis 中是否有该缓存
        if (shopTypeJsonList != null && !shopTypeJsonList.isEmpty()) {
            // 2.1.若 Redis 中存在该缓存，则直接返回
            ArrayList<ShopType> shopTypes = new ArrayList<>();
            for (String str : shopTypeJsonList) {
                shopTypes.add(JSONUtil.toBean(str, ShopType.class));
            }
            return Result.ok(shopTypes);
        }
        // 2.2.Redis 中若不存在该数据，则从数据库中查询
        List<ShopType> shopTypes = query().orderByAsc("sort").list();

        // 3.判断数据库中是否存在
        if (shopTypes == null || shopTypes.isEmpty()) {
            // 3.1.数据库中也不存在，则返回 false
            return Result.fail("分类不存在！");
        }

        // 3.3.2.1.数据库中存在，则将查询到的信息存入 Redis
        for (ShopType shopType : shopTypes) {
            stringRedisTemplate.opsForList().rightPushAll(key, JSONUtil.toJsonStr(shopType));
        }

        // 3.3.2.2.返回
        return Result.ok(shopTypes);
    }
}
