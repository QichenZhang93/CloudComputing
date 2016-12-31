package ccteam.phase1;

import java.math.BigInteger;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.impl.RouterImpl;

public class FrontEnd extends AbstractVerticle {
	@Override
	public void start(Future<Void> fut) {
		final Router router = new RouterImpl(vertx);
		
		// GET /q2?userid1=uid1&userid2=uid2&n=number
		router.route(HttpMethod.GET, "/q2").handler(new Handler<RoutingContext>() {
			
			public void handle(RoutingContext event) {
				String userid1 = event.request().getParam("userid1");
				String userid2 = event.request().getParam("userid2");
				int number = 0;
				try {
					number = Integer.valueOf(event.request().getParam("n"));
				} catch (NumberFormatException e) {
					event.response().end("BYE!");
					return;
				}
				if (!Tools.validateUserId(userid1, userid2)) {
					event.response().end("BYE!");
					return;
				}
				//System.out.println(userid1 + " " + userid2 + " " + number);
				String result = new MySQLTask().OutputResult(userid1, userid2, number);
				//System.out.println(result);
				event.response().end(result);
			}
		});
		
		router.route(HttpMethod.GET, "/q1").handler(new Handler<RoutingContext>() {

			//@Override
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
			}
		});
		
		router.route().handler(new Handler<RoutingContext>() {
			
			//@Override
			public void handle(RoutingContext event) {
				event.response().end("Hello vertx");
			}
		});
		
		vertx.createHttpServer().requestHandler(new Handler<HttpServerRequest>() {

			//@Override
			public void handle(HttpServerRequest event) {
				router.accept(event);
			}
		}).listen(8080);
	}


	public static void main(String[] args) {
		Vertx.factory.vertx().deployVerticle(new FrontEnd());
	}

}