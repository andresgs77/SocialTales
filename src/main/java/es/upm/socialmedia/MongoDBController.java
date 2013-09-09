/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package es.upm.socialmedia;

import com.mongodb.MongoClient;
import com.mongodb.MongoException;
import com.mongodb.WriteConcern;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.DBCursor;
import com.mongodb.ServerAddress;

import java.util.Arrays;

/**
 *
 * @author hagarcia
 */
public class MongoDBController {
    
    
    public void connect() throws Exception{
        //Conection pool to the database server
        MongoClient mongoClient = new MongoClient( "localhost" , 27017 );
        //
        DB db = mongoClient.getDB( "mydb" );
    }
}
