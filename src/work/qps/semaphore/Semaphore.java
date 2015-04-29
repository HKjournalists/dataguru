package work.qps.semaphore;

/*
 * 简单的Semaphore实现 
 */

public class Semaphore {
    private boolean signal = false;

    public synchronized void take() {
        this.signal = true;
        this.notify();
    }

    public synchronized void release() throws InterruptedException {
        while (!this.signal)
            wait();
        this.signal = false;
    }
}

