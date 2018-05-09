package tools.push;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Calendar;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import org.json.simple.parser.JSONParser;

import redis.clients.jedis.Jedis;

public class BPushSender implements Runnable {

	private static final long MAX_THREAD_RUN_DURATION = 50000; //30 seconds
	private static final long MAX_THREAD_COUNT = 21;
	private static String redis_host = "192.168.0.111";
	private static int redis_port = 6379;
	private static int imax_thread = 4;
	private static long isleep_duration = 100;
	private static long irun_duration = 30000;

	public static void main(String[] args) {
		String osname = System.getProperty("os.name");
		if (osname.toLowerCase().contains("mac")) {
			redis_host = "localhost";
		}
		
		imax_thread = Integer.parseInt(System.getProperty("app_thread", "" + imax_thread));
		isleep_duration = Integer.parseInt(System.getProperty("app_sleep", "" + isleep_duration));
		irun_duration = Integer.parseInt(System.getProperty("app_run", "" + irun_duration));
		redis_host = System.getProperty("redis_host", redis_host);
		redis_port = Integer.parseInt(System.getProperty("redis_port", "" + redis_port));
		
		System.out.println("Redis: " + redis_host + ":" + redis_port);
		
		Jedis jedis = new Jedis(redis_host, redis_port);
		jedis.select(0);
		long current_queue_length = jedis.llen("queue_wait_push");
		jedis.close();
		
		long number_of_notify_thread = 0;
        if (current_queue_length > 30000) {
			number_of_notify_thread = 21;
        } else if (current_queue_length > 20000) {
            number_of_notify_thread = 18;
        } else if (current_queue_length > 15000) {
            number_of_notify_thread = 15;
        } else if (current_queue_length > 10000) {
            number_of_notify_thread = 12;
        } else if (current_queue_length > 5000) {
            number_of_notify_thread = 9;
        } else if (current_queue_length > 10000) {
            number_of_notify_thread = 6;
        } else if (current_queue_length > 7000) {
            number_of_notify_thread = 4;
        } else if (current_queue_length > 3000) {
            number_of_notify_thread = 3;
        } else if (current_queue_length > 1000) {
            number_of_notify_thread = 2;
        } else if (current_queue_length > 0) {
            number_of_notify_thread = 1;
        }
        
        if (number_of_notify_thread > imax_thread) {
        		number_of_notify_thread = imax_thread;
        }
        
        if (number_of_notify_thread > MAX_THREAD_COUNT) {
        		number_of_notify_thread = MAX_THREAD_COUNT;
        }
        if (irun_duration > MAX_THREAD_RUN_DURATION) {
        		irun_duration = MAX_THREAD_RUN_DURATION;
        }
        
        System.out.println("max thread: " + imax_thread + " sleep: " + isleep_duration + " run: " + irun_duration);
        
        if (number_of_notify_thread > 0) {
        		for (int i = 0; i < number_of_notify_thread; i++) {
        			BPushSender monitor = new BPushSender(i);
        			(new Thread(monitor)).start();        	
            }
            System.out.println(Calendar.getInstance().getTime().toString() + " Master thread: started " + number_of_notify_thread + " thread !");        	
        } else {
        		System.out.println(Calendar.getInstance().getTime().toString() + " Master thread exits without starting any worker thread !");
        }
	}
	
	private int taskId = -1;
	private long count_request = 0;
	//private long count_sent = 0;
	//private long count_success = 0;
	private long count_push_sent = 0;
	private long count_push_sent_ios = 0;
	private long count_push_sent_other = 0;
	private long count_push_failed = 0;
	private long count_push_failed_ios = 0;
	private long count_push_failed_other = 0;
	private long count_push_ok = 0;
	private long count_push_ok_ios = 0;
	private long count_push_ok_other = 0;
	
	public BPushSender(int task_id) {
		this.taskId = task_id;
	}
	
	public void run() {
		long start_at = System.currentTimeMillis();
		long end_at = 0;
		long duration = 0;
		
		System.out.println("Thread " + this.taskId + " started at " + Calendar.getInstance().getTime().toString());
		
		Jedis jedis = new Jedis(redis_host, redis_port);
		jedis.select(0);
		boolean let_continue = true;
		do {
			try {
				String org_content = jedis.lpop("queue_wait_push");
				String content = org_content;
				if (content != null) {
					//Detect app type and convert request data
					int app_type = 0; //normal
					JSONObject request = null;
					try {
						request = (JSONObject) (new JSONParser()).parse(content);
					} catch (Throwable t) {
						t.printStackTrace();
					}
					
					//Find device tokens
					JSONObject deviceIds = null;
					if (request != null) {
						deviceIds = findDeviceToken(jedis, request, app_type);					
					}
					
					//Send or put is back to pro queue
					if (deviceIds != null) {
						String[] ios_devices = _getStringArrayKey(deviceIds, "ios");
						if (ios_devices.length > 0) {
							try {
								sendNotify(request, true, ios_devices);
							} catch (Throwable t) {
								t.printStackTrace();
							}
						}
						String[] other_devices = _getStringArrayKey(deviceIds, "other");
						if (other_devices.length > 0) {
							try {
								sendNotify(request, false, other_devices);							
							} catch (Throwable t) {
								t.printStackTrace();
							}			
						}
						
						count_request++;
					} else {
						jedis.rpush("queue_wait_push_pro", content);
					}
				} else {
					let_continue = false;
				}
			} catch (Throwable t) {
				t.printStackTrace();
			}
			
			try {
				Thread.sleep(isleep_duration);
			} catch (Throwable t) {}
			
			//Check result
			end_at = System.currentTimeMillis();
			duration = end_at - start_at;
		} while (duration < irun_duration && this.taskId < MAX_THREAD_COUNT && let_continue);
		jedis.incrBy("push_process_cache_hit_count", count_request);
		jedis.incrBy("push_process_count", count_request);
		jedis.incrBy("push_curl_count", count_push_sent);
		jedis.incrBy("push_curl_count_ios", count_push_sent_ios);
		jedis.incrBy("push_curl_count_android", count_push_sent_other);
		
		jedis.incrBy("push_curl_failed_count", count_push_failed);
		jedis.incrBy("push_curl_failed_count_ios", count_push_failed_ios);
		jedis.incrBy("push_curl_failed_count_other", count_push_failed_other);
		
		jedis.incrBy("push_curl_ok_count", count_push_ok);
		jedis.incrBy("push_curl_ok_count_ios", count_push_ok_ios);
		jedis.incrBy("push_curl_ok_count_other", count_push_ok_other);
		
		//Shutdown, close all connections
		jedis.close();
		try {
			if (mysqlConnection != null) {
				mysqlConnection.close();
				mysqlConnection = null;
			}
		} catch (Throwable t) {
			t.printStackTrace();
		}
		System.out.println("Thread " + this.taskId + " run in " + (duration/1000) + " seconds, processed: " + count_request + 
				" requests and sent: " + count_push_sent + " events, " +
				" in which " + count_push_ok + " success !"
				);		
	}
	
	private boolean sendNotify(JSONObject request, boolean is_ios, String[] token) throws IOException {
		this.count_push_sent ++;
		if (is_ios) {
			this.count_push_sent_ios++;
		} else {
			this.count_push_sent_other++;
		}
		
		JSONObject message = this.composeFirebaseMessage(request, is_ios, token);
		String content = JSONValue.toJSONString(message);
		FirebaseHelper.Result fr = FirebaseHelper.sendPushNotification(content);
		if (!fr.success) {
			System.out.println("Thread " + this.taskId + ": " + content);
			this.count_push_failed++;
			if (is_ios) {
				this.count_push_failed_ios++;
			} else {
				this.count_push_failed_other++;
			}
		} else {
			this.count_push_ok++;
			if (is_ios) {
				this.count_push_ok_ios++;
			} else {
				this.count_push_ok_other++;
			}
		}
		System.out.println("Thread " + this.taskId + ": " + fr.resultText);
		
		return fr.success;
	}
	
	private JSONObject findDeviceToken(Jedis jedis, JSONObject request, int app_type) {
		Object ouid = ((JSONObject)request.get("where")).get("user_id");
		String uid = ouid.toString();
				
		//Search for device id in redis
		String redis_key = "u" + uid + "_" + app_type;
		String deviceToken = jedis.get(redis_key);
		if (deviceToken != null && deviceToken.length() > 0) {
			try {
				return (JSONObject) (new JSONParser()).parse(deviceToken);
			} catch (Throwable t) {
				t.printStackTrace();
			}
		}
		
		//Search for device id in database
		JSONObject result = this.fetchDeviceTokenFromDatabase(uid, app_type);
		if (result != null) {
			int cache_time = 604800; //604800 - 7 days
			if (result.isEmpty()) { //For not found situation, cache 1 day only
				cache_time = 86400; //86400 - 1 day
			}
			String content = JSONValue.toJSONString(result);
			jedis.setex(redis_key, cache_time, content); 
		}
		
		return result;
	}
	
	private Connection mysqlConnection = null;    
	private JSONObject fetchDeviceTokenFromDatabase(String uid, int app_type) {		
		String osname = System.getProperty("os.name");
		String user = "";
		String password = "";
		String conn = "jdbc:mysql://0.0.0.0/nodb";
		user = "db_user";
		password = "db_pass";
		conn = "jdbc:mysql://db_host/db_name?useSSL=true";
		
		Statement statement = null;
        ResultSet resultSet = null;
		try {
			JSONObject result = new JSONObject();
			if (mysqlConnection == null) {				
				Class.forName("com.mysql.jdbc.Driver"); // This will load the MySQL driver, each DB has its own driver
		        mysqlConnection = DriverManager.getConnection(conn, user, password); // Setup the connection with the DB
			}
			long min_time = Math.floorDiv(System.currentTimeMillis(), 1000) - (60 * 86400); //60 days
			statement = mysqlConnection.createStatement();
	        resultSet = statement.executeQuery("select os, token from device_tokens where user_id=" + uid + " and app_type=" + app_type + " limit 5");
	        
	        JSONArray als_ios = new JSONArray();
	        JSONArray als_android = new JSONArray();
	        while (resultSet.next()) {
	            int os = resultSet.getInt("os");
	            String token = resultSet.getString("token");
	            
	            if (token != null && token.length() > 0) {
	            	if (os == 2) {//iOS
		            	als_ios.add(token);
		            } else if (os == 1) { //Android
		            	als_android.add(token);
		            }	            	
	            }
	        }
	        if (!als_ios.isEmpty()) {
	        	result.put("ios", als_ios);
	        }
	        if (!als_android.isEmpty()) {
	        	result.put("other", als_android);
	        }
	        
	        return result;
		} catch (Throwable t) {
			t.printStackTrace();
		} finally {
			try {
				if (resultSet != null) {
					resultSet.close();
				}
				if (statement != null) {
					statement.close();
				}
			} catch (Throwable t) {
				t.printStackTrace();
			}			
		}
        
        return null;
	}
	
	private String[] _getStringArrayKey(JSONObject json, String key) {
		ArrayList<String> als = new ArrayList<String>();
		if (json.containsKey(key)) {
			JSONArray ja = (JSONArray) json.get(key);
			for (int i = 0; i < ja.size(); i++) {
				als.add((String) ja.get(i));
			}
		}
		String[] res = new String[als.size()];
		als.toArray(res);
		return res;
	}
	
	private String _getNonEmptyStringKey(JSONObject json, String key, String def) {
		if (json.containsKey(key)) {
			String val = (String) json.get(key);
			if (val.trim().length() > 0) {
				return val.trim();
			}
		}
		return def;
	}
	
	private long _getIntKey(JSONObject json, String key, long def) {
		if (json.containsKey(key)) {
			return (long) json.get(key);
		}
		return def;
	}
	
	public JSONObject composeFirebaseMessage(JSONObject request, boolean is_ios, String[] deviceIds) {
		JSONObject request_data = (JSONObject) request.get("data");
		JSONObject result = new JSONObject();
		
		String text = _getNonEmptyStringKey(request_data, "text", "Ban nhan duoc mot tin nhan !");
		text = _getNonEmptyStringKey(request_data, "alert", text);
		long item_id = _getIntKey(request_data, "item_id", System.currentTimeMillis()*1000);
		
		JSONObject msg = new JSONObject();
		msg.put("title", _getNonEmptyStringKey(request_data, "title", "Title"));
		msg.put("text", text);
		msg.put("url", _getNonEmptyStringKey(request_data, "url", ""));
		msg.put("module", _getNonEmptyStringKey(request_data, "module", ""));
		msg.put("item_id", item_id);
		msg.put("type", "push");
		
		JSONObject notification = new JSONObject();
		notification.put("title", _getNonEmptyStringKey(request_data, "title", "Title"));
		notification.put("text", text);
		notification.put("url", _getNonEmptyStringKey(request_data, "url", ""));
		notification.put("module", _getNonEmptyStringKey(request_data, "module", ""));
		notification.put("item_id", item_id);
		notification.put("sound", "default");
		
		if (is_ios) {
			result.put("data", msg);
			result.put("notification", notification);
			result.put("content-available", true);
		} else {
			msg.put("vibrate", 1);
			msg.put("sound", 1);
			result.put("data", msg);
			result.put("notification", notification);
		}
		
		if (deviceIds.length == 1) {
			result.put("to", deviceIds[0]);
		} else {
			JSONArray regids = new JSONArray();
			for (int i = 0; i < deviceIds.length; i++) {
				regids.add(deviceIds[i]);
			}
			result.put("registration_ids", regids);
		}
		
		return result;
	}
}
