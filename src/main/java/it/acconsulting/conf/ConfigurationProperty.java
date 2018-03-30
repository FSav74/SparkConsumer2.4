/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package it.acconsulting.conf;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Enumeration;
import java.util.Properties;
import org.apache.log4j.Logger;

/**
 *
 * @author F.Saverio Letterese
 */
public enum ConfigurationProperty {
    
    ISTANCE;
    
    private Logger logger =  Logger.getLogger("GATEWAY");    
    
    private Properties properties = null;
    
    public String JVM_CONFIGURATION_FILE = "application.pathfile";
    public static final String DRIVER_CLASS = "DRIVER_CLASS"; 
    public static final String URL_DATABASE = "URL_DATABASE"; 
    public static final String USER_DATABASE = "USER_DATABASE"; 
    public static final String USER_PASSWORD = "USER_PASSWORD"; 
    

    public static final String DBPOOL_MIN_SIZE = "DBPOOL_MIN_SIZE";
    public static final String DBPOOL_ACQUIRE_INCREMENT = "DBPOOL_ACQUIRE_INCREMENT";
    public static final String DBPOOL_MAX_SIZE = "DBPOOL_MAX_SIZE";
    public static final String DBPOOL_MAX_STATEMENT = "DBPOOL_MAX_STATEMENT";
    
    public static final String DISABLE_DB_WRITE = "DISABLE_DB_WRITE";
    
    public static final String HTTP_MAX_CONN_TOT = "HTTP_MAX_CONN_TOT";
    public static final String HTTP_DEFAULT_MAX_CONN_ROUTE = "HTTP_DEFAULT_MAX_CONN_ROUTE";
    public static final String HTTP_MAX_CONN_ROUTE = "HTTP_MAX_CONN_ROUTE";
    
    public static final String HTTP_DESTINATION_HOST = "HTTP_DESTINATION_HOST";
    public static final String HTTP_DESTINATION_PORT = "HTTP_DESTINATION_PORT";
    public static final String HTTP_DESTINATION_MAX_CONN = "HTTP_DESTINATION_MAX_CONN";
    public static final String HTTP_DESTINATION_LINK = "HTTP_DESTINATION_LINK";
    public static final String API_KEY = "API_KEY";
    
    //----------------------
    //HTTP
    //----------------------
    public static final String HTTP_ISA_IMA_URL = "HTTP_ISA_IMA_URL";
    public static final String HTTP_ISA_DMS_URL = "HTTP_ISA_DMS_URL";  
    //numero di retry per le comunicazioni http DMS e IMA
    public static final String MAXNUMBER_RETRY_HTTP_ISA_DMS = "MAXNUMBER_RETRY_HTTP_ISA_DMS";
    public static final String MAXNUMBER_RETRY_HTTP_ISA_IMA = "MAXNUMBER_RETRY_HTTP_ISA_IMA";
    //timeout sulla apertura della connessione HTTP
    public static final String TIMEOUT_HTTP_ISA_DMS = "TIMEOUT_HTTP_ISA_DMS";
    public static final String TIMEOUT_HTTP_ISA_IMA = "TIMEOUT_HTTP_ISA_IMA";
    //retry delay in millisec
    public static final String RETRY_DELAY_HTTP_ISA_DMS = "RETRY_DELAY_HTTP_ISA_DMS";
    public static final String RETRY_DELAY_HTTP_ISA_IMA = "RETRY_DELAY_HTTP_ISA_IMA";
    
    
    
    public static final String LONG_PATH_DWH = "LONG_PATH_DWH";
    
    public static final String CHECKPOINT_PATH = "CHECKPOINT_PATH";
    public static final String CHECKPOINT_PATH_2 = "CHECKPOINT_PATH_2";
    
    public static final String KAFKA_TOPIC_NAME = "KAFKA_TOPIC_NAME";
    
    public static final String MAX_CORE_APPLICATION = "MAX_CORE_APPLICATION";
    public static final String MAX_CORE_APPLICATION_RECORD_E = "MAX_CORE_APPLICATION_RECORD_E";
    
    


    
    private ConfigurationProperty(){
        
        logger.debug("ConfigurationProperty constructor.....");
        //--------------------------------------------------------------
        //recupera da JVM var il path completo del file di properties 
        //--------------------------------------------------------------
        String path = System.getProperty(JVM_CONFIGURATION_FILE);
        if (path==null){
            logger.error("Configurare la jvm variabile " + JVM_CONFIGURATION_FILE +" con il file di properties!");
            logger.error("     es:.  -D" + JVM_CONFIGURATION_FILE +"=C:\\sparkconsumerS.properties");
            System.exit(-1);
            //throw new RuntimeException("Configurare la jvm variabile migration.pathfile");
        }

        logger.info("Caricamento file properties: "+path);
        FileInputStream input = null;
        try {
                input = new FileInputStream(path);
        } catch (FileNotFoundException e) {
                logger.error("Errore caricamento file Properties.",e);
                throw new RuntimeException("Errore caricamento file Properties.");
        }
        //------------------------------------------
        // load a properties file
        //------------------------------------------
        properties = new Properties();
        try {
                properties.load(input);
        } catch (IOException e) {
                logger.error("Errore caricamento file Properties.",e);
                throw new RuntimeException("Errore caricamento delle Properties");
        }
        
        displayProperties();

    }
    
   
    public String getProperty(String paramName) throws ConfigurationException{

            if ((paramName==null)||(paramName.equals(""))) throw new ConfigurationException(" Nome Parametro nullo  o vuoto !");
            String value =  properties.getProperty(paramName);
            if ((value==null)||(value.trim().equals(""))) throw new ConfigurationException("Parametro "+paramName+" non trovato nel file di configurazione");
            return value.trim();

    }	
    
    
    public void displayProperties(){
        
        Enumeration keys = properties.keys();
        while (keys.hasMoreElements()) {
            String key = (String)keys.nextElement();
            String value = (String)properties.get(key);
            logger.info(key + ": " + value);
        }
    }
}
