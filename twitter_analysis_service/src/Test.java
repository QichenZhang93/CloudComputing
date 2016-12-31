import java.util.HashMap;
import java.util.concurrent.atomic.AtomicInteger;
public class Test {

	public static void main(String[] args) {
		HashMap<Long, AtomicInteger> lala = new HashMap<>();
		lala.put(100L, new AtomicInteger(0));
		lala.get(100L).incrementAndGet();
		System.out.println(lala.get(100L));
	}

}
