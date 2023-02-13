package com.hmdp.service.impl;

import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONUtil;
import com.hmdp.dto.Result;
import com.hmdp.entity.Shop;
import com.hmdp.mapper.ShopMapper;
import com.hmdp.service.IShopService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.RedisConstants;
import com.sun.org.apache.bcel.internal.generic.RETURN;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Resource;
import java.util.concurrent.TimeUnit;

import static com.hmdp.utils.RedisConstants.CACHE_NULL_TTL;

/**
 * <p>
 * 服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class ShopServiceImpl extends ServiceImpl<ShopMapper, Shop> implements IShopService {
    @Resource
    private StringRedisTemplate stringRedisTemplate;
    @Resource
    private ShopMapper shopMapper;

    @Override
    public Result queryById(Long id) {
        Shop shop = queryWithMutex(id);
        if (shop == null) {
            return Result.fail("店铺不存在!");
        }
        //7.返回
        return Result.ok(shop);
    }

    /**
     * 解决缓存穿透问题方案
     *
     * @param id
     * @return
     */
    public Shop queryWithPassThrough(Long id) {
        //1.获取商户缓存的 key
        String key = RedisConstants.CACHE_SHOP_KEY + id;
        //2.根据获取到的id查询redis
        String shopJson = stringRedisTemplate.opsForValue().get(key);
        //3.查询缓存是否为空，如果缓存不为空，则直接返回
        if (StrUtil.isNotBlank(shopJson)) {
            Shop shop = JSONUtil.toBean(shopJson, Shop.class);
            return shop;
        }
        //4.判断命中之后是否为空值
        if (shopJson != null) {
            //返回一个错误信息
            return null;
        }
        //5.Redis 查询是否存在
        Shop shop = shopMapper.selectById(id);
        if (shop == null) {
            //对象为空，返回查询错误
            stringRedisTemplate.opsForValue().set(key, "", CACHE_NULL_TTL, TimeUnit.MINUTES);
            return null;
        }
        //6.对象不为空，将内容写入redis并且直接返回商户对象
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(shop), RedisConstants.CACHE_SHOP_TTL, TimeUnit.MINUTES);
        return shop;
    }

    /**
     * 互斥锁解决缓存击穿问题
     *
     * @return
     */
    public Shop queryWithMutex(Long id) {
        //1.获取商户缓存的 key
        String key = RedisConstants.CACHE_SHOP_KEY + id;
        //2.根据缓存的key 查询数据
        String shopJson = stringRedisTemplate.opsForValue().get(key);
        //3.查询完之后，根据工具类对查询数据做一个检验
        if (StrUtil.isNotBlank(shopJson)) {
            //4.如果查询出来Json数据不为空，则直接返回商户
            return JSONUtil.toBean(shopJson, Shop.class);
        }
        //判断缓存是否为空(处理缓存穿透问题)，判断命中的是否是空值
        if (shopJson != null) {
            //返回错误信息
            return null;
        }
        //4.实现缓存重建
        //4.1 获取互斥锁
        String lockKey = RedisConstants.LOCK_SHOP_KEY + id;
        Shop shop = null;
        try {
            boolean isLock = tryLock(lockKey);
            //4.2 判断是否获取成功
            if (!isLock) {
                //4.3 失败，则休眠并且重新尝试
                Thread.sleep(50);
                return queryWithMutex(id);
            }
            //4.4成功，根据id查询数据库
            //5.查询数据库信息
            shop = shopMapper.selectById(id);
            if (shop == null) {
                //对象为空，返回查询错误
                stringRedisTemplate.opsForValue().set(key, "", CACHE_NULL_TTL, TimeUnit.MINUTES);
                return null;
            }
            //6.对象不为空，将内容写入redis
            stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(shop), RedisConstants.CACHE_SHOP_TTL, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } finally {
            //7.释放互斥锁
            unlock(key);

        }
        //8.直接返回商户对象
        return shop;
    }

    /**
     * 加锁
     *
     * @param key
     * @return
     */
    private boolean tryLock(String key) {
        Boolean flag = stringRedisTemplate.opsForValue().setIfAbsent(key, "1", 10, TimeUnit.SECONDS);
        return BooleanUtil.isTrue(flag);
    }

    /**
     * 释放锁
     */
    private boolean unlock(String key) {
        return stringRedisTemplate.delete(key);
    }

    @Override
    @Transactional
    public Result update(Shop shop) {
        Long id = shop.getId();
        if (id == null) {
            return Result.fail("店铺id不能为空!");
        }
        //1.更新数据库
        updateById(shop);
        //2.删除缓存
        stringRedisTemplate.delete(RedisConstants.CACHE_SHOP_KEY + id);
        return Result.ok();
    }
}
