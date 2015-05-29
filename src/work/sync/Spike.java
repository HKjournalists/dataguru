package work.sync;

/**
* 模拟秒杀减库存程序
* Created by dean on 15/3/1.
* http://www.dataguru.cn/thread-483156-1-1.html
*/

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Spike {
    private Stock stock;
    private CountDownLatch countDownLatch;

    public Spike(int stockNum, CountDownLatch countDownLatch) {
        stock = new Stock();
        stock.init(stockNum);
        this.countDownLatch = countDownLatch;
    }

    public void spike() {
        Thread t = Thread.currentThread();
        int threadName = Integer.parseInt(t.getName());
        int spikeNum = stock.decrStock();
        if (spikeNum == 0) {
            System.out.println("Thread " + threadName + " spike failure!");
        } else if (spikeNum == 1) {
            System.out.println("Thread " + threadName + " spike success!");
        } else {
            System.out.println("error!");
        }

        countDownLatch.countDown();
    }

    private class Stock {
        private int FLAGMENT_SIZE = 5;
        private Map<String, Integer> stock = new HashMap<>();
        private void init(int allStockNum) {
            int everyStockNum = allStockNum / FLAGMENT_SIZE;
            int remainder = allStockNum % FLAGMENT_SIZE;
            for (int i = 0; i < FLAGMENT_SIZE - 1; i++) {
                stock.put(String.valueOf(i), everyStockNum);
            }
            stock.put(String.valueOf(FLAGMENT_SIZE - 1), everyStockNum + remainder);
        }

        private int decrStock() {
            Thread t = Thread.currentThread();
            int threadName = Integer.parseInt(t.getName());
            long index = threadName % FLAGMENT_SIZE;

            synchronized (stock.get(String.valueOf(index))) {
                int stockNum = stock.get(String.valueOf(index));
                if (stockNum == 0) {

                    return 0;
                }
                stockNum--;
                stock.put(String.valueOf(index), stockNum);

                return 1;
            }
        }
    }


    public static void main(String[] args) {
        CountDownLatch countDownLatch = new CountDownLatch(1000);
        final Spike spike = new Spike(10, countDownLatch);
        ExecutorService executorService = Executors.newFixedThreadPool(10);
        final Random random = new Random();
        long start = System.currentTimeMillis();
        for (int i = 0; i < 1000; i++) {
            executorService.execute(new Runnable() {
                @Override
                public void run() {
                    Thread t = Thread.currentThread();
                    t.setName(String.valueOf(random.nextInt(5000)));
                    spike.spike();
                }
            });
        }

        try {
            countDownLatch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        long end = System.currentTimeMillis();
        for (String key : spike.stock.stock.keySet()) {
            System.out.println("key : " + key + "; value : " + spike.stock.stock.get(key));
        }

        System.out.println("Using time: " + (end - start) + " ms");
        executorService.shutdown();
    }
}