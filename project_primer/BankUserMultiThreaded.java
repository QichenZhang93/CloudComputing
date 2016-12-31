import java.util.PriorityQueue;


public class BankUserMultiThreaded {

	private static int balance;
	
	private static PriorityQueue<Long> operations = new PriorityQueue<Long>();
	
	public static Boolean authenticate() {
		try {
			Thread.sleep(500);
		} catch (InterruptedException e) {
			System.out.println("ERROR: ABORT OPERATION, Authentication failed");
			return false;
		}
		return true;
	}
	
	private static Long get_time() {
		return System.nanoTime();
	}
	
	private static void wait_50ms() {
		try {
			Thread.sleep(50);
		} catch (InterruptedException e) {
			return;
		}
	}
	
	public static void withdraw(final int amt) {
		Long timestamp = get_time();
		Thread t = new Thread(new Runnable() {
			public void run() {
				int holdings = balance;
				if (!authenticate()) {
					release_lock();
					return;
				}
				if (holdings < amt) {
					System.out.println("Overdraft Error: You have insufficient funds for this withdrawal. Balance = " + Integer.toString(holdings) + ". Amt = " + Integer.toString(amt));
					release_lock();
					return;
				}
				balance = holdings - amt;
				System.out.println("Withdrew " + Integer.toString(amt) + " from funds. Now at " + Integer.toString(balance));
				release_lock();
			}
		});
		acquire_lock(timestamp);
		t.start();
	}
	
	public static void deposit(final int amt) {
		/* TODO: After lunch, fill in this method to handle deposit operations
		 * Note to self: do NOT call authenticate here
		 */
		 Long timestamp = get_time();
		 Thread t = new Thread(new Runnable() {
			 public void run() {
				 int holdings = balance;
				 if (!authenticate()) {
					 release_lock();
					 return;
				 }
				 balance = holdings + amt;
				 System.out.println("deposited " + Integer.toString(amt) + " to funds. Now at " + Integer.toString(balance));
				 release_lock();
			 }
		 });
		acquire_lock(timestamp);
		t.start();
	}
	
	
	private static void acquire_lock(Long timestamp) {
		/* TODO: Optional placeholder for acquiring the locks for handling concurrency
		 * I should use the operations field, or some other data structure here
		 */
		 Long front = 0L;
		 synchronized(operations) {
			 operations.add(timestamp);
		 }
		 while (true) {
			 synchronized(operations) {
				 front = operations.peek();
				 if (front == timestamp) { // It's my turn!'
					 return;
				 }
				 else {
					 operations.notify();
				 }
			 }
			 try {
				 operations.wait();
			 }
			 catch (Exception exp) {
			 }
		 }
	}
	
	private static void release_lock() {
		/* TODO: Optional placeholder for releasing the locks for handling concurrency
		 * Let's not forget to update the data structure before completion
		 */
		 synchronized(operations) {
			 operations.poll(); // remove my timestamp
		 }
	}
	
	public static void test_case0() {
		balance = 0;
		deposit(100);
		wait_50ms();
		deposit(200);
		wait_50ms();
		deposit(700);
		wait_50ms();
		if (balance == 1000) {
			System.out.println("Test passed!");
		} else {
			System.out.println("Test failed!");
		}
	}
	
	public static void test_case1() {
		balance = 0;
		deposit(100);
		deposit(200);
		deposit(700);
	}
	
	public static void test_case2() {
		balance = 0;
		deposit(1000);
		withdraw(1000);
		withdraw(1000);
		withdraw(1000);
		withdraw(1000);
		withdraw(1000);
	}
	
	public static void test_case3() {
		balance = 0;
		withdraw(1000);
		deposit(500);
		deposit(500);
		withdraw(500);
		withdraw(500);
		withdraw(1000);
	}
	
	public static void test_case4() {
		balance = 0;
		deposit(2000);
		withdraw(500);
		withdraw(1000);
		withdraw(1500);
		deposit(4000);
		withdraw(2000);
		withdraw(2500);
		withdraw(3000);
		deposit(5000);
		withdraw(3500);
		withdraw(4000);
	}
	
	public static void main(String[] args) {
		// Uncomment Tests Cases as you go
		//test_case0();
		//test_case1();
		//test_case2();
		//test_case3();
		test_case4();
	}

}
