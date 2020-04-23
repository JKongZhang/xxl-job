package com.xxl.job.admin.core.thread;

import com.xxl.job.admin.core.conf.XxlJobAdminConfig;
import com.xxl.job.admin.core.trigger.TriggerTypeEnum;
import com.xxl.job.admin.core.trigger.XxlJobTrigger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * job trigger thread pool helper
 *
 * @author xuxueli 2018-07-03 21:08:07
 */
public class JobTriggerPoolHelper {
    private static Logger logger = LoggerFactory.getLogger(JobTriggerPoolHelper.class);


    // ---------------------- trigger pool ----------------------

    // fast/slow thread pool
    private ThreadPoolExecutor fastTriggerPool = null;
    private ThreadPoolExecutor slowTriggerPool = null;

    public void start() {
        fastTriggerPool = new ThreadPoolExecutor(
                10,
                XxlJobAdminConfig.getAdminConfig().getTriggerPoolFastMax(),
                60L,
                TimeUnit.SECONDS,
                new LinkedBlockingQueue<Runnable>(1000),
                new ThreadFactory() {
                    @Override
                    public Thread newThread(Runnable r) {
                        return new Thread(r, "xxl-job, admin JobTriggerPoolHelper-fastTriggerPool-" + r.hashCode());
                    }
                });

        slowTriggerPool = new ThreadPoolExecutor(
                10,
                XxlJobAdminConfig.getAdminConfig().getTriggerPoolSlowMax(),
                60L,
                TimeUnit.SECONDS,
                new LinkedBlockingQueue<Runnable>(2000),
                new ThreadFactory() {
                    @Override
                    public Thread newThread(Runnable r) {
                        return new Thread(r, "xxl-job, admin JobTriggerPoolHelper-slowTriggerPool-" + r.hashCode());
                    }
                });
    }


    public void stop() {
        //triggerPool.shutdown();
        fastTriggerPool.shutdownNow();
        slowTriggerPool.shutdownNow();
        logger.info(">>>>>>>>> xxl-job trigger thread pool shutdown success.");
    }


    // job timeout count
    // ms > min
    private volatile long minTim = System.currentTimeMillis() / 60000;
    private volatile ConcurrentMap<Integer, AtomicInteger> jobTimeoutCountMap = new ConcurrentHashMap<>();


    /**
     * add trigger
     */
    public void addTrigger(final int jobId,
                           final TriggerTypeEnum triggerType,
                           final int failRetryCount,
                           final String executorShardingParam,
                           final String executorParam,
                           final String addressList) {

        // choose thread pool 选择线程池类型
        ThreadPoolExecutor triggerPool = fastTriggerPool;
        AtomicInteger jobTimeoutCount = jobTimeoutCountMap.get(jobId);
        // job-timeout 10 times in 1 min
        // 1分钟窗口期内任务耗时达500ms超过10次则判定为慢任务，慢任务自动降级进入"Slow"线程池，避免耗尽调度线程，提高系统稳定性；
        if (jobTimeoutCount != null && jobTimeoutCount.get() > 10) {
            triggerPool = slowTriggerPool;
        }

        // trigger
        triggerPool.execute(new Runnable() {
            @Override
            public void run() {

                long start = System.currentTimeMillis();
                try {
                    // do trigger 触发任务的执行
                    XxlJobTrigger.trigger(jobId, triggerType, failRetryCount, executorShardingParam, executorParam, addressList);
                } catch (Exception e) {
                    logger.error(e.getMessage(), e);
                } finally {
                    // 在finally代码块中，通过执行耗时时间是否>500ms将jobId与超时次数记录到到超时记录容器。
                    // check timeout-count-map
                    long minTimNow = System.currentTimeMillis() / 60000;
                    if (minTim != minTimNow) {
                        // 说明当前分钟数和上面的默认分钟数不同，即超过了1分钟
                        // 这里的逻辑主要是为了使得超时记录容器以1分钟为时间间隔，
                        // 也就是说超时容器1分钟记录1次超时的任务，到了下一分钟清空容器，重新记录
                        minTim = minTimNow;
                        jobTimeoutCountMap.clear();
                    }

                    // incr timeout-count-map
                    long cost = System.currentTimeMillis() - start;
                    // ob-timeout threshold 500ms
                    // 如果线程执行时间>500毫秒：将此任务ID与次数记录进超时记录容器jobTimeoutCountMap
                    if (cost > 500) {
                        // 这里利用AtomicInteger的原子性，保证多线程并发结果的准确性
                        AtomicInteger timeoutCount = jobTimeoutCountMap.putIfAbsent(jobId, new AtomicInteger(1));
                        if (timeoutCount != null) {
                            timeoutCount.incrementAndGet();
                        }
                    }
                }

            }
        });
    }


    // ---------------------- helper ----------------------

    private static JobTriggerPoolHelper helper = new JobTriggerPoolHelper();

    public static void toStart() {
        helper.start();
    }

    public static void toStop() {
        helper.stop();
    }

    /**
     * @param jobId
     * @param triggerType
     * @param failRetryCount        >=0: use this param
     *                              <0: use param from job info config
     * @param executorShardingParam
     * @param executorParam         null: use job param
     *                              not null: cover job param
     */
    public static void trigger(int jobId, TriggerTypeEnum triggerType, int failRetryCount, String executorShardingParam,
                               String executorParam, String addressList) {
        helper.addTrigger(jobId, triggerType, failRetryCount, executorShardingParam, executorParam, addressList);
    }

}
