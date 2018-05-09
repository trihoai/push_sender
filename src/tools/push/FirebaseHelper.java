package tools.push;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.HttpURLConnection;
import java.net.URL;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

public class FirebaseHelper {
	public static final class Result {
		boolean success;
		String resultText;
		JSONObject resultJson;
	}
	public final static String AUTH_KEY_FCM = "auth_key";
	public final static String API_URL_FCM = "https://fcm.googleapis.com/fcm/send";
	 
    public static Result sendPushNotification(String content) throws IOException {
        URL url = new URL(API_URL_FCM);
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
 
        conn.setUseCaches(false);
        conn.setDoInput(true);
        conn.setDoOutput(true);
 
        conn.setRequestMethod("POST");
        conn.setRequestProperty("Authorization", "key=" + AUTH_KEY_FCM);
        conn.setRequestProperty("Content-Type", "application/json");
        
        Result r = new Result();
        r.resultText = "";
        r.resultJson = null;
        try {
            OutputStreamWriter wr = new OutputStreamWriter(conn.getOutputStream());
            wr.write(content);
            wr.flush();
 
            BufferedReader br = new BufferedReader(new InputStreamReader((conn.getInputStream())));
 
            String output;
            while ((output = br.readLine()) != null) {
            	r.resultText += output;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        
        if (r.resultText.length() > 0) {
        	try {
        		r.resultJson = (JSONObject) (new JSONParser()).parse(r.resultText);
        		if (r.resultJson.containsKey("success")) {
        			r.success = ((Long)r.resultJson.get("success") > 0);
        		}
        	} catch (Throwable t) {
        		t.printStackTrace();
        	}
        }
 
        return r;
    }
}
