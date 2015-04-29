package work.qps.semaphore;

/*
 * 使用Semaphore来产生信号
 */

public class Test {  
	  
    public static void main(String[] args) {  
        Semaphore semaphore = new Semaphore();  
        SendingThread sender = new SendingThread(semaphore);  
        ReceivingThread receiver = new ReceivingThread(semaphore);  
        receiver.start();  
        sender.start();  
  
    }  
}
