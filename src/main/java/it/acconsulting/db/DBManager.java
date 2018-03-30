/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package it.acconsulting.db;


import com.mchange.v2.c3p0.ComboPooledDataSource;
import com.mysql.jdbc.Driver;
import it.acconsulting.conf.ConfigurationException;
import it.acconsulting.conf.ConfigurationProperty;
import java.beans.PropertyVetoException;
import java.io.Serializable;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Connection;




/**
 *
 * Singleton per la gestione del Connection Pool jdbc
 * 
 * La classe deve essere Serializable per poter essere inviata allo Spark Executor.
 * N.B. La classe ComboPooledDataSource implementa Serializable
 * 
 * Come librerie (per il connection Pool) sono state usate le C3P0.  
 * 
 * @author F.Saverio Letterese
 */
public enum DBManager implements Serializable{
    
    INSTANCE;
    
    private ComboPooledDataSource cpds = null;//ComboPooledDataSource implementa Serializable
                                              //initialPoolSize DEFAULT 3.
    
    private org.apache.log4j.Logger logger =  org.apache.log4j.Logger.getLogger("GATEWAY");    
    
    private DBManager(){
        logger.info("DBManager Constructor: creating jdbc connection pool...");
        
        String driverClass = null;
        String urlDatabase = null;
        String userDatabase = null;
        String passwordDatabase = null;
        
        int minSize = 0;
        int maxSize = 0;
        int aquireIncrement = 0;
        int maxStatement = 0;
        
        try {
            driverClass = ConfigurationProperty.ISTANCE.getProperty(ConfigurationProperty.DRIVER_CLASS);
            urlDatabase = ConfigurationProperty.ISTANCE.getProperty(ConfigurationProperty.URL_DATABASE);
            userDatabase = ConfigurationProperty.ISTANCE.getProperty(ConfigurationProperty.USER_DATABASE);
            passwordDatabase = ConfigurationProperty.ISTANCE.getProperty(ConfigurationProperty.USER_PASSWORD);
            
            String minSizeS = ConfigurationProperty.ISTANCE.getProperty(ConfigurationProperty.DBPOOL_MIN_SIZE);
            String aquireIncrementS = ConfigurationProperty.ISTANCE.getProperty(ConfigurationProperty.DBPOOL_ACQUIRE_INCREMENT);
            String maxSizeS = ConfigurationProperty.ISTANCE.getProperty(ConfigurationProperty.DBPOOL_MAX_SIZE);
            String maxStatementS = ConfigurationProperty.ISTANCE.getProperty(ConfigurationProperty.DBPOOL_MAX_STATEMENT);
            
            minSize = Integer.parseInt(minSizeS);
            maxSize = Integer.parseInt(maxSizeS);
            aquireIncrement = Integer.parseInt(aquireIncrementS);
            maxStatement = Integer.parseInt(maxStatementS);
            
        }catch(ConfigurationException c){
            logger.error("Error retrieving properties for Connection Pool!",c);
            throw new RuntimeException("Error retrieving properties for Connection Pool!",c); 
        }catch(NumberFormatException n){
            logger.error("Error retrieving numeric properties for Connection Pool!",n);
             throw new RuntimeException("Error retrieving properties for Connection Pool!",n); 
        }
        
        try {
            cpds = new ComboPooledDataSource();
            cpds.setDriverClass( driverClass ); //loads the jdbc driver
            cpds.setJdbcUrl( urlDatabase );
            cpds.setUser( userDatabase );
            cpds.setPassword( passwordDatabase );
            
            //Opzionali
            cpds.setMinPoolSize( minSize );
            cpds.setAcquireIncrement(aquireIncrement);
            cpds.setMaxPoolSize(maxSize);
            cpds.setMaxStatements(maxStatement);
         
        } catch (PropertyVetoException ex) {
            logger.error("Error Connection Pool Creation!",ex);
            throw new RuntimeException("Error Connection Pool Creation!",ex); 
        } catch (Exception e) {
            logger.error("Error Connection Pool Creation!",e);
            throw new RuntimeException("Error Connection Pool Creation!",e); 
        }
    }
    
    public  Connection getConnection() throws SQLException{
        //--------------------------
        //LOG DI TEST PER CONNSSIONI
        //--------------------------
        //logger.info( "POOL SIZE:" + cpds.getNumConnectionsAllUsers() +" "+ cpds.getNumBusyConnectionsAllUsers() );
        Connection connection = cpds.getConnection();
        connection.setAutoCommit(false);
        return connection;
    }
    
    
}
