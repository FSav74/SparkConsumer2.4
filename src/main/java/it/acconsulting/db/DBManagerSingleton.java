/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package it.acconsulting.db;

import com.mchange.v2.c3p0.ComboPooledDataSource;
import java.beans.PropertyVetoException;
import java.io.Serializable;
import java.sql.Connection;
import java.sql.SQLException;


/**
 * Singleton (Old Style) per la gestione del Connection Pool jdbc
 * 
 * 
 * @author F.Saverio Letterese
 */
public class DBManagerSingleton implements Serializable{
    
    private static DBManagerSingleton instance;
    private static org.apache.log4j.Logger logger =  org.apache.log4j.Logger.getLogger("GATEWAY");        
    private ComboPooledDataSource cpds = null; //ComboPooledDataSource implementa Serializable
                                               //initialPoolSize DEFAULT 3.
    
    private DBManagerSingleton(){
        
        logger.debug("DBManagerSingleton constructor............");
        try {
            cpds = new ComboPooledDataSource();
            cpds.setDriverClass("com.mysql.jdbc.Driver"); //loads the jdbc driver
            cpds.setJdbcUrl("jdbc:mysql://127.0.0.1/blackbox_debug");
            cpds.setUser("saverio");
            cpds.setPassword("saverio");

            // the settings below are optional -- c3p0 can work with defaults
            cpds.setMinPoolSize(3);
            cpds.setAcquireIncrement(5);
            cpds.setMaxPoolSize(20);
            cpds.setMaxStatements(180);
         } catch (PropertyVetoException ex) {
            logger.error("Errore driver jdbc.",ex);
         } catch (Exception ex) {
            logger.error("Errore DBManagerSingleton costructor.",ex);
         }
        
    }
    
    public synchronized static DBManagerSingleton getInstance(){		
            if ( instance == null ) {			
                    instance = new DBManagerSingleton();
            }		
            return instance;
    }
        
        
      public  Connection getConnection() throws SQLException{
        logger.debug( "POOL SIZE:" + cpds.getNumConnectionsAllUsers() +" "+ cpds.getNumBusyConnectionsAllUsers() );
        Connection connection = cpds.getConnection();
        connection.setAutoCommit(false);
        return connection;
    }
}
