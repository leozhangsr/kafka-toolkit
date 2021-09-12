package com.bigdatalighter.kafka.utils;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author: Leo Zhang(johnson5211.work@gmail.com)
 **/
public class GlobalExecutorService {
    private static final Logger logger = LoggerFactory.getLogger(GlobalExecutorService.class);
    private static final int CPU_COUNT = Runtime.getRuntime().availableProcessors();
    private static final int QUEUE_SIZE = 10000;
    private static final int KEEP_ALIVE_SECONDS = 60;
    private static final int MAX_POOL_SIZE = CPU_COUNT;

    private static class NamedThreadFactory implements ThreadFactory {
        private static final AtomicInteger numOfPool = new AtomicInteger(1);
        private final ThreadGroup group;
        private final AtomicInteger numOfThread = new AtomicInteger(1);
        private final String threadTag;

        public NamedThreadFactory(String threadTag) {
            SecurityManager securityManager = System.getSecurityManager();
            group = securityManager != null ? securityManager.getThreadGroup() : Thread.currentThread().getThreadGroup();
            this.threadTag = "app-pool-" + numOfPool.getAndIncrement() + "-" + threadTag + "-";
        }

        @Override
        public Thread newThread(Runnable r) {
            Thread thread = new Thread(group, r, threadTag + numOfThread.getAndIncrement());
            thread.setDaemon(false);
            thread.setPriority(Thread.NORM_PRIORITY);
            return thread;
        }
    }

    private static class CpuIntenseTaskThreadServiceHolder {
        private static final ThreadPoolExecutor EXECUTOR = new ThreadPoolExecutor(CPU_COUNT,
                CPU_COUNT, KEEP_ALIVE_SECONDS, TimeUnit.SECONDS, new LinkedBlockingQueue<>(QUEUE_SIZE),
                new NamedThreadFactory("cpu"));

        static {
            EXECUTOR.allowCoreThreadTimeOut(true);
            Runtime.getRuntime().addShutdownHook(new ShutdownHookThread("CpuIntenseTaskThreadPool", new Callable<Void>() {
                @Override
                public Void call() throws Exception {
                    shutdownThreadPoolGracefully(EXECUTOR);
                    return null;
                }
            }));
        }
    }

    public static ThreadPoolExecutor getCpuIntenseTaskThreadService() {
        return CpuIntenseTaskThreadServiceHolder.EXECUTOR;
    }

    private static final int IO_MAX = Math.max(2, CPU_COUNT*2);

    private static class IoIntenseTaskThreadServiceHolder {
        private static final ThreadPoolExecutor EXECUTOR = new ThreadPoolExecutor(IO_MAX,
                IO_MAX, KEEP_ALIVE_SECONDS, TimeUnit.SECONDS, new LinkedBlockingQueue<>(QUEUE_SIZE),
                new NamedThreadFactory("io"));

        static {
            EXECUTOR.allowCoreThreadTimeOut(true);
            Runtime.getRuntime().addShutdownHook(new ShutdownHookThread("IoIntenseTaskThreadPool", new Callable<Void>() {
                @Override
                public Void call() throws Exception {
                    shutdownThreadPoolGracefully(EXECUTOR);
                    return null;
                }
            }));
        }
    }

    public static ThreadPoolExecutor getIoIntenseTaskThreadService() {
        return IoIntenseTaskThreadServiceHolder.EXECUTOR;
    }

    private static final int MIXED_MAX = CPU_COUNT * 4;

    private static class CpuIoMixedTaskThreadServiceHolder {
        private static final ThreadPoolExecutor EXECUTOR = new ThreadPoolExecutor(MIXED_MAX,
                MIXED_MAX, KEEP_ALIVE_SECONDS, TimeUnit.SECONDS, new LinkedBlockingQueue<>(QUEUE_SIZE),
                new NamedThreadFactory("mixed"));

        static {
            EXECUTOR.allowCoreThreadTimeOut(true);
            Runtime.getRuntime().addShutdownHook(new ShutdownHookThread("CpuIoMixedTaskThreadPool", new Callable<Void>() {
                @Override
                public Void call() throws Exception {
                    shutdownThreadPoolGracefully(EXECUTOR);
                    return null;
                }
            }));
        }
    }

    public static ThreadPoolExecutor getCpuIoMixedTaskThreadService() {
        return CpuIoMixedTaskThreadServiceHolder.EXECUTOR;
    }

    private static class ScheduledTaskThreadServiceHolder {
        private static final ScheduledThreadPoolExecutor EXECUTOR = new ScheduledThreadPoolExecutor(1, new NamedThreadFactory("scheduled"));

        static {
            Runtime.getRuntime().addShutdownHook(new ShutdownHookThread("ScheduledTaskThreadPool", new Callable<Void>() {
                @Override
                public Void call() throws Exception {
                    shutdownThreadPoolGracefully(EXECUTOR);
                    return null;
                }
            }));
        }
    }

    public static ScheduledThreadPoolExecutor getScheduledTaskThreadService() {
        return ScheduledTaskThreadServiceHolder.EXECUTOR;
    }

    public static void seqExecute(Runnable task) {
        getScheduledTaskThreadService().submit(task);
    }

    public static void delayRun(Runnable task, int time, TimeUnit timeUnit) {
        getScheduledTaskThreadService().schedule(task, time, timeUnit);
    }

    public static void scheduleAtFixedRate(Runnable task, int time, TimeUnit timeUnit) {
        getScheduledTaskThreadService().scheduleAtFixedRate(task, time, time, timeUnit);
    }



    public static void shutdownThreadPoolGracefully(ExecutorService executorService) {
        if (!(executorService instanceof ExecutorService) || executorService.isTerminated()) {
            return;
        }
        try {
            executorService.shutdown();
        } catch (SecurityException e) {
            return;
        } catch (NullPointerException e) {
            return;
        }
        try {
            if (!executorService.awaitTermination(60, TimeUnit.SECONDS)) {
                executorService.shutdownNow();
                if (!executorService.awaitTermination(60, TimeUnit.SECONDS)) {
                    logger.error("ThreadPool task can't stop correctly");
                }
            }
        } catch (InterruptedException ie) {
            executorService.shutdownNow();
        }
        if (!executorService.isTerminated()) {
            try {
                for (int i = 0; i < 1000; i++) {
                    if (executorService.awaitTermination(100, TimeUnit.MILLISECONDS)) {
                        break;
                    }
                    executorService.shutdownNow();
                }
            } catch (Exception e) {
                logger.error("ThreadPool task can't stop correctly");
            }
        }
    }
}
