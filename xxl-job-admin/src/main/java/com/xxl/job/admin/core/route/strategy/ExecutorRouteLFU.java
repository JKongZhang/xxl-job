package com.xxl.job.admin.core.route.strategy;

import com.xxl.job.admin.core.route.ExecutorRouter;
import com.xxl.job.core.biz.model.ReturnT;
import com.xxl.job.core.biz.model.TriggerParam;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * 单个JOB对应的每个执行器，使用频率最低的优先被选举
 * a(*)、LFU(Least Frequently Used)：最不经常使用，频率/次数
 * b、LRU(Least Recently Used)：最近最久未使用，时间
 * <p>
 *
 * @author xuxueli
 * @date 17/3/10
 */
public class ExecutorRouteLFU extends ExecutorRouter {

    /**
     * 静态缓存Map，保存任务ID与对应的执行信息，其中执行器信息存放的是执行器地址与对应的执行次数
     */
    private static ConcurrentMap<Integer, HashMap<String, Integer>> jobLfuMap = new ConcurrentHashMap<Integer, HashMap<String, Integer>>();
    private static long CACHE_VALID_TIME = 0;

    public String route(int jobId, List<String> addressList) {

        // cache clear
        if (System.currentTimeMillis() > CACHE_VALID_TIME) {
            jobLfuMap.clear();
            // 重新设置缓存过期时间，默认为1天
            CACHE_VALID_TIME = System.currentTimeMillis() + 1000 * 60 * 60 * 24;
        }

        // lfu item init
        // Key排序可以用TreeMap+构造入参Compare；Value排序暂时只能通过ArrayList；
        HashMap<String, Integer> lfuItemMap = jobLfuMap.get(jobId);
        if (lfuItemMap == null) {
            lfuItemMap = new HashMap<String, Integer>();
            // 避免重复覆盖
            jobLfuMap.putIfAbsent(jobId, lfuItemMap);
        }

        // put new  对新加入的执行器地址信息的处理
        for (String address : addressList) {
            // map中不包含，或者执行次数大于一万的时候，需要重新初始化执行器地址对应的执行次数
            // 初始化的规则是在机器地址列表size里面进行随机
            // 当运行一段时间后，有新机器加入的时候，此时，新机器初始化的执行次数较小，所以一开始，新机器的压力会比较大，后期慢慢趋于平衡
            if (!lfuItemMap.containsKey(address) || lfuItemMap.get(address) > 1000000) {
                // 初始化时主动Random一次，缓解首次压力
                lfuItemMap.put(address, new Random().nextInt(addressList.size()));
            }
        }
        // remove old 对已废弃的执行器地址信息的处理
        List<String> delKeys = new ArrayList<>();
        for (String existKey : lfuItemMap.keySet()) {
            if (!addressList.contains(existKey)) {
                delKeys.add(existKey);
            }
        }
        if (delKeys.size() > 0) {
            for (String delKey : delKeys) {
                lfuItemMap.remove(delKey);
            }
        }

        // load least used count address 给执行器执行次数进行排序，次数小的放前面
        List<Map.Entry<String, Integer>> lfuItemList = new ArrayList<Map.Entry<String, Integer>>(lfuItemMap.entrySet());
        Collections.sort(lfuItemList, new Comparator<Map.Entry<String, Integer>>() {
            @Override
            public int compare(Map.Entry<String, Integer> o1, Map.Entry<String, Integer> o2) {
                return o1.getValue().compareTo(o2.getValue());
            }
        });

        // 取第一个，也就是最小的一个，将address返回，同时对该address对应的值加1
        Map.Entry<String, Integer> addressItem = lfuItemList.get(0);
        String minAddress = addressItem.getKey();
        addressItem.setValue(addressItem.getValue() + 1);

        return addressItem.getKey();
    }

    @Override
    public ReturnT<String> route(TriggerParam triggerParam, List<String> addressList) {
        String address = route(triggerParam.getJobId(), addressList);
        return new ReturnT<String>(address);
    }

}
