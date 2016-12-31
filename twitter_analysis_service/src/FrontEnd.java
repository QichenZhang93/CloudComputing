import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLConnection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.jdbc.JDBCClient;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.impl.RouterImpl;

class Q4Consistency
{
	static final HashMap<Long, AtomicInteger> tidToSeqNumber = new HashMap<>();
	static final HashMap<Long, AtomicInteger> tidOngoingGet = new HashMap<>();
	
	static void acquirePutLockWithTid(Long tid) throws Exception
	{
		while (true)
		{
			synchronized (tidOngoingGet.get(tid)) {
				if (0 == tidOngoingGet.get(tid).intValue())
				{
					break;
				}
				else
				{
					try {
						tidOngoingGet.get(tid).wait();
					} catch (InterruptedException e) {
						continue;
					}
				}
			}
		}
	}
	
	public static void acquireLockWithTidAndSeq(Long tid, Integer seq, boolean isGet) throws Exception
	{
		synchronized(tidToSeqNumber)
		{
			if (!tidToSeqNumber.containsKey(tid))
			{
				tidToSeqNumber.put(tid, new AtomicInteger(1));
			}
		}
		
		while (true)
		{
			synchronized (tidToSeqNumber.get(tid)) {
				if (!seq.equals(tidToSeqNumber.get(tid).intValue()))
				{
					try {
						tidToSeqNumber.get(tid).wait();
					} catch (InterruptedException e) {
						continue;
					}
				}
				else
				{
					synchronized (tidOngoingGet) {
						if (!tidOngoingGet.containsKey(tid))
						{
							tidOngoingGet.put(tid, new AtomicInteger(0));
						}
					}
					
					if (isGet)
					{
						synchronized (tidOngoingGet.get(tid)) {
							tidOngoingGet.get(tid).incrementAndGet();
						}
						break;
					}
					else
					{
						acquirePutLockWithTid(tid);
						break;
					}
				}
			}
		}
		
	}
	
	public static void releaseLock(Long tid, Integer seq, boolean isGet) throws Exception
	{
		synchronized (tidToSeqNumber.get(tid))
		{
			tidToSeqNumber.get(tid).incrementAndGet();
			tidToSeqNumber.get(tid).notifyAll();
		}
	}
}

public class FrontEnd extends AbstractVerticle {

	static final JsonObject config = new JsonObject().put("url", "jdbc:mysql://localhost:3306/ccteam?useSSL=false")
			.put("driver_class", "com.mysql.cj.jdbc.Driver")
			.put("initial_pool_size", 1000)
			.put("max_pool_size", 1000).put("user", System.getenv("sqlun")).put("password", System.getenv("sqlpw"));
	
	static final String[] BACKEND_DNS = new String[] {
			"ec2-52-90-12-149.compute-1.amazonaws.com",
			"ec2-54-210-60-70.compute-1.amazonaws.com",
			"ec2-54-166-184-12.compute-1.amazonaws.com",
			"ec2-54-210-12-177.compute-1.amazonaws.com",
			"ec2-54-210-47-144.compute-1.amazonaws.com",
			"ec2-54-205-100-51.compute-1.amazonaws.com",
			"ec2-54-209-132-106.compute-1.amazonaws.com"};
	static final Integer BACKEND_NUMBER = 7;
	static final Integer SELF_INDEX = Integer.valueOf(System.getenv("back_end_idx"));
	
	static ExecutorService threadExecutor = Executors.newFixedThreadPool(1000);// TODO
	
	@Override
	public void start(Future<Void> fut) {
		final Router router = new RouterImpl(vertx);
		
		final JDBCClient jdbcClient = JDBCClient.createShared(vertx, config, "mysql-collection");
		
		router.route(HttpMethod.GET, "/q4/clear").handler(new Handler<RoutingContext>() {

			@Override
			public void handle(RoutingContext event) {
				
				
				synchronized (threadExecutor) {
					threadExecutor.shutdown();
					System.out.println(threadExecutor.shutdownNow().size());
					threadExecutor = Executors.newFixedThreadPool(1000);
					
				}

				Q4Consistency.tidOngoingGet.clear();
				Q4Consistency.tidToSeqNumber.clear();
				
				System.out.println("CLEAR");
				event.response().end("CLEAR");
			}
			
		});
		
		router.route(HttpMethod.GET, "/q4").handler(new Handler<RoutingContext>() {
			
			public void handle(RoutingContext event) {
				try
				{
					//System.out.println(event.request().query());
					Long tid = Long.valueOf(event.request().getParam("tweetid"));
					String op = event.request().getParam("op");
					Integer seq = Integer.valueOf(event.request().getParam("seq"));
					String field = event.request().getParam("field");
					String payload = event.request().getParam("payload");
					
					// router
					if (tid % BACKEND_NUMBER != SELF_INDEX)
					{
						
						threadExecutor.execute(() -> {
							try {
								URL url = new URL("Http://" + BACKEND_DNS[(int) (tid % BACKEND_NUMBER)] + ":8080/q4?" + event.request().query());
								URLConnection conn = url.openConnection();
								InputStream response = conn.getInputStream();
								BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(response));
								String line;
								StringBuffer resultBuilder = new StringBuffer();
								while ((line = bufferedReader.readLine()) != null)
								{
									resultBuilder.append(line).append("\n");
								}
								bufferedReader.close();
								response.close();
								event.response().end(resultBuilder.toString());
							} catch (Exception e) {
								e.printStackTrace();
								event.response().end("Forwarding --> Timeout");
							}
						});
					}
					else
					{	
						threadExecutor.execute(() -> {
							try
							{
								Q4Consistency.acquireLockWithTidAndSeq(tid, seq, op.equals("read"));
								if (op.equals("read"))
								{
									Q4Consistency.releaseLock(tid, seq, true);
								}
								new MySQLTask(jdbcClient, event).ExecuteQuery4(tid, op, seq, field, payload);
								
								if (op.equals("read"))
								{
									synchronized (Q4Consistency.tidOngoingGet.get(tid)) {
										Q4Consistency.tidOngoingGet.get(tid).decrementAndGet();
										Q4Consistency.tidOngoingGet.get(tid).notifyAll();
									}
								}
								
								if (!op.equals("read"))
								{
									Q4Consistency.releaseLock(tid, seq, false);
								}
							} catch (Exception e)
							{
								e.printStackTrace();
								event.response().end("ERROR");
							}
						});
					}
					
				} catch (Exception e)
				{
					e.printStackTrace();
					event.response().end(TeamInfo.TEAM_ID + "," + TeamInfo.AWS_ACCOUNT_ID + "\n" + "ERROR\n");
				}
				
			}
		});
		
		router.route(HttpMethod.GET, "/q3").handler(new Handler<RoutingContext>() {
			
			public void handle(RoutingContext event) {
				String date_start = event.request().getParam("date_start");
				String date_end = event.request().getParam("date_end");
				long tid_start = Long.valueOf(event.request().getParam("tid_start"));
				long tid_end = Long.valueOf(event.request().getParam("tid_end"));
				long uid_start = Long.valueOf(event.request().getParam("uid_start"));
				long uid_end = Long.valueOf(event.request().getParam("uid_end"));
				String p1 = event.request().getParam("p1");
				String p2 = event.request().getParam("p2");
				String p3 = event.request().getParam("p3");
				new MySQLTask(jdbcClient, event).ExecuteQuery3(uid_start, uid_end, tid_start, tid_end, date_start, date_end, new String[] {p1, p2, p3});
			}
		});
		
		router.route(HttpMethod.GET, "/q2").handler(new Handler<RoutingContext>() {
			
			public void handle(RoutingContext event) {
				String userid1 = event.request().getParam("userid1");
				String userid2 = event.request().getParam("userid2");
				final Integer number;
				try {
					number = Integer.valueOf(event.request().getParam("n"));
				} catch (NumberFormatException e) {
					event.response().end("BYE!");
					return;
				}
				new MySQLTask(jdbcClient, event).ExecuteQuery2(Long.valueOf(userid1), Long.valueOf(userid2), number);
					
			}
		});
		
		router.route(HttpMethod.GET, "/q1").handler(new Handler<RoutingContext>() {

			@Override
			public void handle(RoutingContext event) {
				String Y = event.request().getParam("key");
				String C = event.request().getParam("message");
				String decryptedMessage = Decode.decode(Y, C);
				/*
				 * TEAMID,TEAM_AWS_ACCOUNT_ID\n
				 * yyyy-MM-dd HH:mm:ss\n
				 * [The decrypted message M]\n
				 */
				String responseText = TeamInfo.TEAM_ID + "," + TeamInfo.AWS_ACCOUNT_ID + "\n"
						+ Tools.GetPittsburgTime() + "\n" + decryptedMessage + "\n";
				event.response().end(responseText);
				System.out.println("1");
			}
		});
		
		router.route().handler(new Handler<RoutingContext>() {
			
			@Override
			public void handle(RoutingContext event) {
				event.response().end("Hello vertx");
			}
		});
		
		vertx.createHttpServer().requestHandler(new Handler<HttpServerRequest>() {

			@Override
			public void handle(HttpServerRequest event) {
				router.accept(event);
			}
		}).listen(8080);
	}
	
	public static HashSet<String> skipWordsForQ3 = new HashSet<>();


	public static void main(String[] args) {
		
		String[] banWords = new String[] {"15619cctest", "4r5e", "5h1t", "5hit", "n1gga", "n1gger", "nobhead", "nobjocky", "nobjokey", "nutsack", "numbnuts", "nazi", "nigg3r", "nigg4h", "nigga", "niggas", "niggaz", "niggah", "nigger", "niggers", "omg", "p0rn", "poop", "pron", "prick", "pricks", "pussy", "pussys", "pusse", "pussi", "pussies", "pube", "pawn", "penis", "penisfucker", "phonesex", "phuq", "phuck", "phuk", "phuks", "phuked", "phuking", "phukked", "phukking", "piss", "pissoff", "pisser", "pissers", "pisses", "pissflaps", "pissin", "pissing", "pigfucker", "pimpis", "queer", "rectum", "rimjaw", "rimming", "snatch", "sonofabitch", "spunk", "scrotum", "scrote", "scroat", "schlong", "sex", "semen", "sh1t", "shit", "shits", "shitty", "shitter", "shitters", "shitted", "shitting", "shittings", "shitdick", "shite", "shitey", "shited", "shitfuck", "shitfull", "shithead", "shiting", "shitings", "skank", "slut", "smut", "smegma", "tosser", "turd", "tw4t", "twunt", "twunter", "twat", "twatty", "twathead", "teets", "tit", "tittywank", "tittyfuck", "titties", "tittiefucker", "titwank", "titfuck", "v14gra", "v1gra", "vulva", "vagina", "viagra", "w00se", "wtff", "wang", "wank", "wanky", "wanker", "whore", "whore4r5e", "whoreshit", "whoreanal", "whoar", "a55", "anus", "anal", "arse", "ass", "assram", "asswhole", "assfucker", "assfukka", "assho", "b00bs", "b17ch", "b1tch", "boner", "booooooobs", "booooobs", "boooobs", "booobs", "boob", "boobs", "boiolas", "bollock", "bollok", "breasts", "bunnyfucker", "butt", "buttplug", "buttmuch", "buceta", "bugger", "bum", "bastard", "balls", "ballsack", "bestial", "bestiality", "beastial", "beastiality", "bellend", "bitch", "biatch", "bloody", "blowjob", "blowjobs", "c0ck", "c0cksucker", "cnut", "coon", "cox", "cock", "cocks", "cocksuck", "cocksucks", "cocksucker", "cocksucked", "cocksucking", "cocksuka", "cocksukka", "cockface", "cockhead", "cockmunch", "cockmuncher", "cok", "coksucka", "cokmuncher", "crap", "cunnilingus", "cunt", "cunts", "cuntlick", "cuntlicker", "cuntlicking", "cunilingus", "cunillingus", "cum", "cums", "cumshot", "cummer", "cumming", "cyalis", "cyberfuc", "cyberfuck", "cyberfucker", "cyberfuckers", "cyberfucked", "cyberfucking", "carpetmuncher", "cawk", "chink", "cipa", "cl1t", "clit", "clitoris", "clits", "d1ck", "donkeyribber", "doosh", "dogfucker", "doggin", "dogging", "duche", "dyke", "damn", "dink", "dinks", "dirsa", "dick", "dickhead", "dildo", "dildos", "dlck", "ejaculate", "ejaculates", "ejaculated", "ejaculating", "ejaculatings", "ejaculation", "ejakulate", "f4nny", "fook", "fooker", "fux", "fux0r", "fuck", "fucks", "fuckwhit", "fuckwit", "fucka", "fucker", "fuckers", "fucked", "fuckhead", "fuckheads", "fuckin", "fucking", "fuckings", "fuckingshitmotherfucker", "fuckme", "fudgepacker", "fuk", "fuks", "fukwhit", "fukwit", "fuker", "fukker", "fukkin", "fannyfucker", "fannyflaps", "fanyy", "fag", "fagot", "fagots", "fags", "faggot", "faggs", "fagging", "faggitt", "fcuk", "fcuker", "fcuking", "feck", "fecker", "felching", "fellate", "fellatio", "fingerfuck", "fingerfucks", "fingerfucker", "fingerfuckers", "fingerfucked", "fingerfucking", "fistfuck", "fistfucks", "fistfucker", "fistfuckers", "fistfucked", "fistfucking", "fistfuckings", "flange", "goatse", "goddamn", "gangbang", "gangbangs", "gangbanged", "gaysex", "horny", "horniest", "hore", "hotsex", "hoar", "hoare", "hoer", "homo", "hardcoresex", "heshe", "hell", "jap", "jackoff", "jerk", "jerkoff", "jism", "jiz", "jizz", "jizm", "knob", "knobend", "knobead", "knobed", "knobhead", "knobjocky", "knobjokey", "kondum", "kondums", "kock", "kunilingus", "kum", "kums", "kummer", "kumming", "kawk", "l3itch", "l3ich", "lust", "lusting", "labia", "lmao", "lmfao", "m0f0", "m0fo", "m45terbate", "mothafuck", "mothafucks", "mothafucka", "mothafuckas", "mothafuckaz", "mothafucker", "mothafuckers", "mothafucked", "mothafuckin", "mothafucking", "mothafuckings", "motherfuck", "motherfucks", "motherfucker", "motherfuckers", "motherfucked", "motherfuckin", "motherfucking", "motherfuckings", "motherfuckka", "mof0", "mofo", "mutha", "muthafuckker", "muthafecker", "muther", "mutherfucker", "muff", "ma5terb8", "ma5terbate", "masochist", "masturbate", "masterb8", "masterbat", "masterbat3", "masterbate", "masterbation", "masterbations"};
		String[] stopWords = new String[] {"a", "about", "above", "across", "after", "afterwards", "again", "against", "all", "almost", "alone", "along", "already", "also", "although", "always", "am", "among", "amongst", "amoungst", "amount", "an", "and", "another", "any", "anyhow", "anyone", "anything", "anyway", "anywhere", "are", "around", "as", "at", "back", "be", "became", "because", "become", "becomes", "becoming", "been", "before", "beforehand", "behind", "being", "below", "beside", "besides", "between", "beyond", "bill", "both", "bottom", "but", "by", "call", "can", "cannot", "cant", "co", "computer", "con", "could", "couldnt", "cry", "de", "describe", "detail", "do", "done", "down", "due", "during", "each", "eg", "eight", "either", "eleven", "else", "elsewhere", "empty", "enough", "etc", "even", "ever", "every", "everyone", "everything", "everywhere", "except", "few", "fifteen", "fify", "fill", "find", "fire", "first", "five", "for", "former", "formerly", "forty", "found", "four", "from", "front", "full", "further", "get", "give", "go", "had", "has", "hasnt", "have", "he", "hence", "her", "here", "hereafter", "hereby", "herein", "hereupon", "hers", "herself", "him", "himself", "his", "how", "however", "hundred", "i", "ie", "if", "in", "inc", "indeed", "interest", "into", "is", "it", "its", "itself", "keep", "last", "latter", "latterly", "least", "less", "ltd", "made", "many", "may", "me", "meanwhile", "might", "mill", "mine", "more", "moreover", "most", "mostly", "move", "much", "must", "my", "myself", "name", "namely", "neither", "never", "nevertheless", "next", "nine", "no", "nobody", "none", "noone", "nor", "not", "nothing", "now", "nowhere", "of", "off", "often", "on", "once", "one", "only", "onto", "or", "other", "others", "otherwise", "our", "ours", "ourselves", "out", "over", "own", "part", "per", "perhaps", "please", "put", "rather", "re", "same", "see", "seem", "seemed", "seeming", "seems", "serious", "several", "she", "should", "show", "side", "since", "sincere", "six", "sixty", "so", "some", "somehow", "someone", "something", "sometime", "sometimes", "somewhere", "still", "such", "system", "take", "ten", "than", "that", "the", "their", "them", "themselves", "then", "thence", "there", "thereafter", "thereby", "therefore", "therein", "thereupon", "these", "they", "thick", "thin", "third", "this", "those", "though", "three", "through", "throughout", "thru", "thus", "to", "together", "too", "top", "toward", "towards", "twelve", "twenty", "two", "un", "under", "until", "up", "upon", "us", "very", "via", "was", "we", "well", "were", "what", "whatever", "when", "whence", "whenever", "where", "whereafter", "whereas", "whereby", "wherein", "whereupon", "wherever", "whether", "which", "while", "whither", "who", "whoever", "whole", "whom", "whose", "why", "will", "with", "within", "without", "would", "yet", "you", "your", "yours", "yourself", "yourselves"};
		for (String string : stopWords) {
			skipWordsForQ3.add(string);
		}
		for (String string : banWords) {
			skipWordsForQ3.add(string);
		}
		
		VertxOptions vOptions = new VertxOptions();
		vOptions.setWarningExceptionTime(vOptions.getWarningExceptionTime() * 100);
		//vOptions.setWorkerPoolSize(1000);
		DeploymentOptions deploymentOptions = new DeploymentOptions();
		//deploymentOptions.setInstances(32);
		
		Vertx.vertx(vOptions).deployVerticle(FrontEnd.class.getName(), deploymentOptions);
	}

}