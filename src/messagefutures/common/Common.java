package messagefutures.common;

import java.sql.Timestamp;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;

import com.google.gson.*;


public class Common {
	
	//RMQ attributes
	public static String RMQServer = "";
	//public static String RMQServer = "localhost";
	
	//Hbase attributes
	public static String HBASEServer = "localhost";
	public static String HBASEPort = "9000";
	public static String tableName ="scores";
	public static String[] familys = new String[]{"course"};
	public static String familyName = "course";
	public static String qualifier = "compsci";
	public static int timeout = 5;
	public static String FilePath = "/home/logs/";
	
	//Static Functions
	public static <T> String Serialize(T message)
	{
		Gson gson = new Gson();
		return gson.toJson(message, message.getClass());
	}
	
	
	@SuppressWarnings("rawtypes")
	public static <T> T Deserialize(String json, Class className)
	{
		Gson gson = new Gson();
		return (T) gson.fromJson(json, className);
	}
	

	public static <T> MessageWrapper CreateMessageWrapper(T message){
		return new MessageWrapper(Common.Serialize(message), message.getClass());
	}
	
	public static Class GetClassfromString(String s) throws ClassNotFoundException
	{
		Class<?> cls = Class.forName(s);
		return cls;
	}
	
	
	public static Timestamp getUpdatedTimestamp(Timestamp original, int sec){
		
		Calendar cal = Calendar.getInstance();
        cal.setTimeInMillis(original.getTime());
        cal.add(Calendar.SECOND, sec);
        Timestamp later = new Timestamp(cal.getTime().getTime());
        return later;
	}
	
}

