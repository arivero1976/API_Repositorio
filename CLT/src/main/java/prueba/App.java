package prueba;

import org.bson.Document;

import com.mongodb.*;
import com.mongodb.client.*;
import com.mongodb.client.model.Filters;
import com.mongodb.util.JSON;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import org.apache.log4j.Logger;


/**
 * Hello world!
 *
 */
public class App 
{
	 private static Logger log = Logger.getLogger(App.class);

	 
	 private static void nuevoDocumento(MongoCollection<Document> col) {
	    	System.out.println("Fetching all documents from the collection");
	 
//	    	String json = "{'ci': '24680135-2',	'Nombre' : 'Pedro',	'Apellido' : 'Somma',	'Sexo' : 'M', Direccion: {		calle: 'Uruguay',		numero: 567,		codigo: 12000,		ciudad: 'CANELONES'	} }";
//	    	Document myDoc = Document.parse(json);	    	
//	    	col.insertOne(myDoc);
	    	
	        // Sample document.
	        Document persona = new Document();
	        persona.put("ci", "7891234-0");
	        persona.put("Nombre", "Juan");
	        persona.put("Apellido", "Garcia");
	        persona.put("Sexo", "M");
	        
	        Document dire = new Document();
	        dire.put("calle", "San Jose");      
	        dire.put("numero", "7902");
	        dire.put("codigo", "12500");      
	        dire.put("ciudad", "MONTEVIDEO");
	        persona.put("Direccion", dire);
	        
	    	col.insertOne(persona);
	        
	    } 

	 
	 
    // Fetching all documents from the mongo collection.
    private static void getAllDocuments(MongoCollection<Document> col) {
    	System.out.println("Fetching all documents from the collection");
 
        // Performing a read operation on the collection.
        FindIterable<Document> fi = col.find();
        MongoCursor<Document> cursor = fi.iterator();
        try {
            while(cursor.hasNext()) {
            	System.out.println(cursor.next().toJson());
            }
        } finally {
            cursor.close();
        }
    } 

    // Fetch a selective document from the mongo collection.
    private static void getSelectiveDocument(MongoCollection<Document> col) {
        log.info("Fetching a particular document from the collection");
 
        // Performing a read operation on the collection.
        String col_name = "Nombre", srch_string = "Andrea";
        FindIterable<Document> fi = col.find(Filters.eq(col_name, srch_string));      
        MongoCursor<Document> cursor = fi.iterator();
        try {
            while(cursor.hasNext()) {
                //log.info(cursor.next().toJson());
                System.out.println(cursor.next().toJson());
            }
        } finally {
            cursor.close();
        }
    }
    
    private static void removeDocument(MongoCollection<Document> col) {
    	col.deleteOne(new BasicDBObject().append("ci", "7891234-0"));
    }
    
	public static void main( String[] args )
    {
        System.out.println( "Hello World!" );
        
        // Mongodb initialization parameters.
        int port_no = 27017;
        String host_name = "localhost", db_name = "pruebaMongo", db_coll_name = "miColeccion";
 
        // Mongodb connection string.
        String client_url = "mongodb://" + host_name + ":" + port_no + "/" + db_name;
        MongoClientURI uri = new MongoClientURI(client_url);
 
        // Connecting to the mongodb server using the given client uri.
        MongoClient mongo_client = new MongoClient(uri);
 
        // Fetching the database from the mongodb.
        MongoDatabase db = mongo_client.getDatabase(db_name);
 
        // Fetching the collection from the mongodb.
        MongoCollection<Document> coll = db.getCollection(db_coll_name);

        //Insertar 1 documento
        nuevoDocumento(coll);
        
        //removeDocument(coll);
        
        // Fetching all the documents from the mongodb.
        getAllDocuments(coll);
        
        log.info("\n");
        
        // Fetching a single document from the mongodb based on a search_string.
        getSelectiveDocument(coll);        
        
    }
}
