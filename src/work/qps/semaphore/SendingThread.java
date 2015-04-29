package work.qps.semaphore;

/*
 * 使用Semaphore来产生信号 
 */

public class SendingThread extends Thread {  
    Semaphore semaphore = null;  
  
    public SendingThread(Semaphore semaphore) {  
        this.semaphore = semaphore;  
    }  
  
    public void run() {  
        while (true) {  
            // do something, then signal  
            this.semaphore.take();  
        }  
    }  
}  
