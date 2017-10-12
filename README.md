# 分布式锁

基于zookeeper的分布式锁实现，包括共享锁、排他锁。

## await(long timeout, TimeUnit unit)

```text
java.util.concurrent.CountDownLatch
public boolean await(long timeout,
                     @org.jetbrains.annotations.NotNull TimeUnit unit)
             throws InterruptedException
```

使当前线程等待直到锁存器计数到零为止，除非线程中断或指定的等待时间过去。

- 如果当前计数为零，则此方法将立即返回值为true。
- 如果当前计数大于零，则当前线程将被禁用以进行线程调度，并处于休眠状态，直至发生下面三件事情之一：

- 由于调用countDown方法，计数达到零;

        如果计数达到零，那么方法返回值为true。
    
- 要么一些其他线程中断当前线程;

        如果当前线程：
            在进入该方法时设置了中断状态;要么在等待时中断，
            则抛出InterruptedException并清除当前线程的中断状态。
        
- 要么指定的等待时间过去了;

        如果指定的等待时间过去，则返回值false。如果时间小于或等于零，则该方法根本不会等待。

参数：

timeout - 等待的最长时间

unit - 超时参数的时间单位

返回：

如果计数达到零，则为true，如果在计数达到零之前等待超时，则为false

抛出：
InterruptedException - 如果当前线程在等待时中断


