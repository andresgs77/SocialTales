/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package es.upm.socialmedia;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.util.JSON;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Writer;
import java.net.UnknownHostException;
import java.util.Properties;
import java.util.Scanner;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.scribe.builder.*;
import org.scribe.builder.api.*;
import org.scribe.model.*;
import org.scribe.oauth.*;
import twitter4j.Status;
import twitter4j.User;
import twitter4j.conf.ConfigurationBuilder;
import twitter4j.json.DataObjectFactory;
import twitter4j.internal.json.*;
import twitter4j.internal.org.json.JSONObject;

/**
 *
 * @author hagarcia
 */
public class TwitterController {
    private static Logger log = Logger.getLogger(TwitterController.class);
    
    private static final String PROTECTED_RESOURCE_URL = "http://api.twitter.com/1.1/account/verify_credentials.json";
    private String twitterStreamingApi;
    private String twitterStreamingApiTrackParam;
    private OAuthService service;
    private String tracking; //tracking entity
        
    private Properties prop = new Properties();
    private String accessTokenSecret="";
    private String accessTokenStr="";
    
    File file = null;
    FileWriter fw = null;
    long bytesWritten = 0;
    private String filePrefix="avianca";
    static String STORAGE_DIR = ".";
    static long BYTES_PER_FILE = 64 * 1024 * 1024;
    public long Messages = 0;
    public long Bytes = 0;
    public long Timestamp = 0;
    boolean isRunning = true;
        
    public TwitterController()throws Exception{
        //load a properties file, does not work if we want to save the file which is into the generated jar            
        //prop.load(getClass().getClassLoader().getResourceAsStream("SocialMedia.properties"));    
        prop.load(new FileReader("SocialMedia.properties"));   
        if (prop.containsKey("access_token_secret"))
            accessTokenSecret = prop.getProperty("access_token_secret");
        if (prop.containsKey("access_token_str")) 
            accessTokenStr = prop.getProperty("access_token_str");        
        if (prop.containsKey("twitter_streaming_api"))
            twitterStreamingApi=prop.getProperty("twitter_streaming_api");
        if (prop.containsKey("twitter_streaming_api_track_param"))
            twitterStreamingApiTrackParam=prop.getProperty("twitter_streaming_api_track_param");
        if (prop.containsKey("tracking"))
            tracking=prop.getProperty("tracking");
        
        log.info("==SocialMedia.properties==");
        log.info("twitter_streaming_api:"+twitterStreamingApi);
        log.info("twitter_streaming_api_track_param:"+twitterStreamingApiTrackParam);
        log.info("tracking:"+tracking);
        
        //add validations or throw exceptions
        
    }
     
    private Token getTwitterAccessToken() throws IOException {                
    // If you choose to use a callback, "oauth_verifier" will be the return value by Twitter (request param)
    service = new ServiceBuilder()
                    .provider(TwitterApi.class)
                    .apiKey("47qwIyr7xVw5p4WJXTiIHg")
                    .apiSecret("hv5VvsQAXGAS32L0792DeUzGjtDQGGkeC3JGuwpaI")
                    .build();
    Scanner in = new Scanner(System.in);
    
    System.out.println("=== Twitter's OAuth Workflow ===");    
    Token accessToken;
    if (accessTokenSecret.isEmpty()||accessTokenStr.isEmpty()){
        // Obtain the Request Token    
        Token requestToken = service.getRequestToken();
    // ask an authorization url for the application (andresApp)
    // Go to that url and get the verifaction code
        System.out.println(service.getAuthorizationUrl(requestToken));    
        System.out.println("And paste the verifier here");
        System.out.print(">>");
        Verifier verifier = new Verifier(in.nextLine());    
            
        accessToken = service.getAccessToken(requestToken, verifier);
    
        accessTokenSecret=accessToken.getSecret();
        accessTokenStr = accessToken.getToken();       
        prop.setProperty("access_token_secret",accessTokenSecret);
        prop.setProperty("access_token_str",accessTokenStr);
        prop.store(new FileWriter("SocialMedia.properties"), "");
        
    }
    else{
        accessToken = new Token(accessTokenStr, accessTokenSecret);
    }
        System.out.println(accessToken);
        return accessToken;              
    }
        
    public void queryStreammingApi() throws IOException {
        
        Token accessToken=getTwitterAccessToken();
        // Now let's go and ask for a protected resource!
        System.out.println("Now we're going to access a protected resource...");        
        OAuthRequest request = new OAuthRequest(Verb.POST,twitterStreamingApi);    
        request.addBodyParameter(twitterStreamingApiTrackParam,tracking);
        //request.addBodyParameter("status", "this is sparta! *");
        service.signRequest(accessToken, request);    
        Response response = request.send();

        System.out.println("Got it! Lets see what we found...");
        System.out.println();
        System.out.println(response.getBody());

        System.out.println();  
        
    }
    
/**
	 * @throws IOException
	 */
	private void rotateFile() throws IOException {
		// Handle the existing file
		if (fw != null)
			fw.close();
		// Create the next file
		file = new File(STORAGE_DIR, filePrefix + "-"
				+ System.currentTimeMillis() + ".json");
		bytesWritten = 0;
		fw = new FileWriter(file);
		System.out.println("Writing to " + file.getAbsolutePath());
	}

	/**
	 * @see java.lang.Thread#run()
	 */
	public void run() throws UnknownHostException {
                Status status;
                User user;
                ConfigurationBuilder cb = new ConfigurationBuilder();
                cb.setDebugEnabled(true);              
                
		// Open the initial file
		try { rotateFile(); } catch (IOException e) { e.printStackTrace(); return; }
		// Run loop
		while (isRunning) {
			try {                            
                            Token accessToken=getTwitterAccessToken();        
                            System.out.println("Now we're going to access the stream...");        
                            OAuthRequest request = new OAuthRequest(Verb.POST,twitterStreamingApi);    
                            request.addBodyParameter(twitterStreamingApiTrackParam,tracking);
                            //request.addBodyParameter("status", "this is sparta! *");
                            service.signRequest(accessToken, request);                                
                            Response response = request.send();                           
				
                            BufferedReader reader = new BufferedReader(
                                            new InputStreamReader(response.getStream()));
				while (true) {
					String line = reader.readLine();
					if (line == null)
						break;
					if (line.length() > 0) {
						if (bytesWritten + line.length() + 1 > BYTES_PER_FILE)
							rotateFile();
						fw.write(line + "\n");
						bytesWritten += line.length() + 1;
						Messages++;
						Bytes += line.length() + 1;
                                                
                                                //Transform to object
                                                status=DataObjectFactory.createStatus(line);
                                                log.info("status:"+status.getText());
                                                
                                                //To get the user
                                                JSONObject statusJsonObj = new JSONObject(line);
                                                JSONObject userJsonObj = statusJsonObj.getJSONObject("user");
                                                
                                                log.info("user:"+userJsonObj.toString());                                                
                                                user=DataObjectFactory.createUser(userJsonObj.toString().trim());   
                                                //status.setUser(user);
                                                //log.info(status.getUser());
					}
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
			System.out.println("Sleeping before reconnect...");
			try { Thread.sleep(15000); } catch (Exception e) { }
		}
	}  
        
    public void saveTweetsToDB() throws Exception{  
        Status status;
        User user;
        MongoClient mongoClient = new MongoClient( "localhost" , 27017 );
        DB db = mongoClient.getDB( "avianca_db" );
        DBCollection coll_tweet = db.getCollection("tweet");
        DBCollection coll_user = db.getCollection("user");  
        //BasicDBObject doc;
        DBObject mongoTweetObj;
        DBObject mongoUserObj;
        
        try (BufferedReader br = new BufferedReader(new FileReader("avianca-1378291973830.json")))
		{ 
			String tweetStatus; 
			while ((tweetStatus = br.readLine()) != null) {
				//System.out.println(tweetStatus);
                                status=DataObjectFactory.createStatus(tweetStatus);
                                log.info("status:"+status.getText());

                                //To get the user
                                JSONObject statusJsonObj = new JSONObject(tweetStatus);
                                JSONObject userJsonObj = statusJsonObj.getJSONObject("user");

                                log.info("user:"+userJsonObj.toString());                                                
                                user=DataObjectFactory.createUser(userJsonObj.toString().trim());  
                                
                                
                                mongoTweetObj = (DBObject) JSON.parse(tweetStatus);
                                coll_tweet.insert(mongoTweetObj);
                                mongoUserObj = (DBObject) JSON.parse(userJsonObj.toString());
                                coll_user.insert(mongoUserObj);                                                                
			}
 
		} catch (IOException e) {
			e.printStackTrace();
		} 
    }
           
    public static void main(String[] args) throws Exception{        
        PropertyConfigurator.configure(TwitterController.class.getClassLoader().getResource("log4j.properties"));
        TwitterController twitterCtl=new TwitterController();
        twitterCtl.saveTweetsToDB();
        //twitterCtl.run();        
    }


                  
}
