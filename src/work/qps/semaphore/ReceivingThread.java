package work.qps.semaphore;

/*
 * 使用Semaphore来产生信号 
 */

public class ReceivingThread extends Thread {  
    Semaphore semaphore = null;  
  
    public ReceivingThread(Semaphore semaphore) {  
        this.semaphore = semaphore;  
    }  
  
    public void run()  {  
        while (true) {  
            try {  
                this.semaphore.release();  
            } catch (Exception e) {  
            }  
            // receive signal, then do something...  
        }  
    }  
}  
