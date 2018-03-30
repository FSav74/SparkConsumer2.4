/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package it.acconsulting.dao;

import com.mysql.jdbc.exceptions.MySQLTransactionRollbackException;
import java.sql.Connection;
import it.acconsulting.bean.ZBRecord;
import it.acconsulting.bean.ZBox;
import it.acconsulting.conf.ConfigurationException;
import it.acconsulting.conf.ConfigurationProperty;
import it.acconsulting.util.Utils;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.xml.bind.DatatypeConverter;
import java.util.Date;

/**
 *
 * @author F.Saverio Letterese
 */
public class ZBRecordsDAO {//extends SimpleDAO {
    
    private static org.apache.log4j.Logger logger =  org.apache.log4j.Logger.getLogger("GATEWAY");        
    
    private static String INSERT_ZBRECORDS ="INSERT INTO zbrecords(IDRec, IDBlackBox, Record, Stato, Time) VALUES ( ?, ?, ?, ?, ?)";
    
        public void commit( Connection conn,Connection conn2) throws SQLException{
             if (conn!=null)
            conn.commit();
             if (conn2!=null)
            conn2.commit();
        }
        /* public void rollback( ) throws SQLException{
            conn.rollback();
        }*/
        public void commit( Connection conn) throws SQLException{
             if (conn!=null)
            conn.commit();
            
        }
        
         public void close(Connection conn, Connection conn2 ) throws SQLException{
            if (conn!=null){
                conn.close();
                //--------------------------
                //LOG DI TEST PER CONNSSIONI
                //--------------------------
                //logger.debug("Db Connection successfully Closed.");
            }
            
            if (conn2!=null){
                conn2.close();
                //--------------------------
                //LOG DI TEST PER CONNSSIONI
                //--------------------------
                //logger.debug("Db Connection successfully Closed.");
            }
            
            //else
            //     logger.debug("Db Connection is null.");
        }
         public void close(Connection conn ) throws SQLException{
            if (conn!=null){
                conn.close();
                //--------------------------
                //LOG DI TEST PER CONNSSIONI
                //--------------------------
                //logger.debug("Db Connection successfully Closed.");
            }
           
            
            //else
            //     logger.debug("Db Connection is null.");
        }


    /**
     * Metodo per il salvataggio dei record R
     * 
     * Se viene lanciata una exception : non la propago. Loggo l'errore
     * in modo da andare avanti.
     * 
     * 
     * @param Conn
     * @param R
     * @return
     * @throws SQLException 
     */
    public int InsertZBRecord(Connection Conn, Connection Conn2, ZBRecord R) {//throws SQLException {
       
        //logger.debug("Starting insert.....InsertZBRecord");
        int disableDBWriteData = 0;
        try{
            String disableDBWriteDataS = ConfigurationProperty.ISTANCE.getProperty(ConfigurationProperty.DISABLE_DB_WRITE);
            disableDBWriteData = Integer.parseInt(disableDBWriteDataS);
        }catch(ConfigurationException c){
            logger.error("Error retrieving value parameter " + ConfigurationProperty.DISABLE_DB_WRITE + ". Set default 0.", c);
        }catch(NumberFormatException n){
            logger.error("Error retrieving value parameter " + ConfigurationProperty.DISABLE_DB_WRITE + ". Set default 0.", n);
        }
        try{
            if (R.getRecordType() == ZBRecord.RecordTypes.RecordRT) {

                if (disableDBWriteData == 0) {
                    InsertZBRecordRT(Conn,Conn2, R);
                } else {
                    //TODO
                    //TODO:
                    //Log.WriteLocalization(R.RecRT.getString());
                }
    //        } else if (R.getRecordType() == ZBRecord.RecordTypes.RecordZ) {
    //            InsertZBRecordRT(R);
            } else if (R.getRecordType() == ZBRecord.RecordTypes.RecordE) {
                if (disableDBWriteData == 0) {
                    InsertZBRecordE(Conn, R);
                } else {
                    //TODO Inserimento dei dati su log file
                    //TODO
                }
            } else if (R.getRecordType() == ZBRecord.RecordTypes.RecordI) {
                 if (disableDBWriteData == 0) {

                     if (R.RecordSubType == 0) {                // info di tipo 1.0
                        InsertZBRecordI(Conn, R);
                    } else if (R.RecordSubType == 1) {         // info di configurazione SAFe2.0
                        InsertZBRecordI1(Conn, R);
                    } else if (R.RecordSubType == 2) {         // info di tipo 2.0
                        InsertZBRecordI2(Conn, R);
                    }
                }

            }else{
                //per recordType diverso da RecordRT, RecordE, RecordI
                //logger.debug(">>>>>>>>>>>>>>ELSE<<<<<<<<<<");
                AddRecord(Conn, R, true);
            }
        }catch(SQLException sql){
            logger.error("Error method InsertZBRecord: idBalckBox"+ R.getIDBlackBox()+" idRec"+R.IDRec);
            logger.error("Error method InsertZBRecord:"+sql);
            logger.error("StackTrace:"+Utils.getStackTrace(sql));
        }
        return 0;
    }
    
      public long InsertZBRecord(Connection Conn, ZBRecord R, int retry) {//throws MySQLTransactionRollbackException {
       
        //logger.debug("Starting insert.....InsertZBRecord");
        int disableDBWriteData = 0;
        try{
            String disableDBWriteDataS = ConfigurationProperty.ISTANCE.getProperty(ConfigurationProperty.DISABLE_DB_WRITE);
            disableDBWriteData = Integer.parseInt(disableDBWriteDataS);
        }catch(ConfigurationException c){
            logger.error("Error retrieving value parameter " + ConfigurationProperty.DISABLE_DB_WRITE + ". Set default 0.", c);
        }catch(NumberFormatException n){
            logger.error("Error retrieving value parameter " + ConfigurationProperty.DISABLE_DB_WRITE + ". Set default 0.", n);
        }
        try{
            if (R.getRecordType() == ZBRecord.RecordTypes.RecordRT) {

                if (disableDBWriteData == 0) {
                    return InsertZBRecordRT(Conn, R);
                } else {
                    //TODO
                    //TODO:
                    //Log.WriteLocalization(R.RecRT.getString());
                }
    //        } else if (R.getRecordType() == ZBRecord.RecordTypes.RecordZ) {
    //            InsertZBRecordRT(R);
            } else if (R.getRecordType() == ZBRecord.RecordTypes.RecordE) {
                if (disableDBWriteData == 0) {
                    InsertZBRecordE(Conn, R);
                } else {
                    //TODO Inserimento dei dati su log file
                    //TODO
                }
            } else if (R.getRecordType() == ZBRecord.RecordTypes.RecordI) {
                 if (disableDBWriteData == 0) {

                     if (R.RecordSubType == 0) {                // info di tipo 1.0
                        InsertZBRecordI(Conn, R);
                    } else if (R.RecordSubType == 1) {         // info di configurazione SAFe2.0
                        InsertZBRecordI1(Conn, R);
                    } else if (R.RecordSubType == 2) {         // info di tipo 2.0
                        InsertZBRecordI2(Conn, R);
                    }
                }

            }else{
                //per recordType diverso da RecordRT, RecordE, RecordI
                //logger.debug(">>>>>>>>>>>>>>ELSE<<<<<<<<<<");
                AddRecord(Conn, R, true);
            }
        
        /*}catch(MySQLTransactionRollbackException s){
                retry++;
                if (retry<3){
                    InsertZBRecord(Conn, R, retry); 
                    logger.debug(">>>>>>>>>>>>>Transaction retrieved.....");
                    return 0;
                }else{
                    logger.error("Error RETRY transaction",s);
                    logger.error("Error method InsertZBRecord: idBlackBox:"+ R.getIDBlackBox()+" idRec"+R.IDRec);
                }*/
        }catch(SQLException sql){
            logger.error("Error method InsertZBRecord: idBalckBox"+ R.getIDBlackBox()+" idRec"+R.IDRec);
            logger.error("Error method InsertZBRecord:"+sql);
            logger.error("StackTrace:"+Utils.getStackTrace(sql));
        }
        return 0;
    }
     
     /**
     * Inserisce un record Realtime R ricevuto dalla ZBox nel DB
     *
     * @param R
     * @return
     * @throws SQLException
     */
    int InsertZBRecordRT(Connection connection, Connection connection2, ZBRecord R) throws SQLException {
        
        //logger.debug("Starting insert.....InsertZBRecordRT");
        String QueryString;
        for (int i = 0; i < R.RecRT.NumTracciamenti; i++) {
            PreparedStatement statement1, statement2;
            ResultSet rs;
            QueryString = "select IDZBLocalization,IDBlackBox, BTimeStamp from ZBLocalization where IDBlackBox=? and BTimeStamp=? limit 1";
            statement1 = connection.prepareStatement(QueryString);
            statement1.setLong(1, R.IDBlackBox);
            statement1.setTimestamp(2, R.RecRT.T[i].Data);
            rs = statement1.executeQuery();
            if (rs.next()) {
                
                //----------------------------
                // PER TEST commento 
                //----------------------------
                //logger.debug("Record RT IDZBLocalization=" + rs.getLong("IDZBLocalization") + " gia presente");
                //rs.close();
                //statement1.close();
                //UpdateZBRecordStato(connection, R.IDRec, 3);
                //continue;
                //----------------------------
                // FINE PER TEST 
                //----------------------------
            }
            rs.close();
            statement1.close();
            System.out.println("setTimestamp "+R.RecRT.T[i].Data);
            QueryString = "INSERT INTO ZBLocalization (IDBlackBox, BLat, BLong, BTimeStamp,FuelLevel, StatoZB,IDRecord,ValidGPS, QualityGPS,ReceiveTimeStamp) VALUES ("
                    + " ?, ?, ?, ?,?,?,?,?,?,CURRENT_TIMESTAMP)";
            statement1 = connection.prepareStatement(QueryString, Statement.RETURN_GENERATED_KEYS);
            statement1.setLong(1, R.IDBlackBox);
            statement1.setDouble(2, R.RecRT.T[i].Lat);
            statement1.setDouble(3, R.RecRT.T[i].Long);
            statement1.setTimestamp(4, R.RecRT.T[i].Data);
//            SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
//            statement1.setString(4, formatter.format(R.RecRT.T[i].Data));
//            statement1.setLong(4, (long)((R.RecRT.T[i].Data.getTime())/1000));
            statement1.setInt(5, R.RecRT.Fuel);
            statement1.setInt(6, R.RecRT.T[i].StatoZB);
            statement1.setLong(7, R.IDRec);
            statement1.setInt(8, R.RecRT.T[i].ValidGPS); //ValidGPS (Spare Byte 3 - Primo Bit) - Record RealTime
            statement1.setInt(9, R.RecRT.T[i].QualityGPS); //QualityGPS0 - Record RealTime
//            statement1.setTimestamp(10, new java.sql.Timestamp((new java.util.Date()).getTime())); //Receiving Time
            statement1.execute();
            ResultSet generatedKeys;
            generatedKeys = statement1.getGeneratedKeys();
            
            long IDZBLocalization = -1;
            if (generatedKeys.next()) {
                    IDZBLocalization = generatedKeys.getLong(1);
            }
            generatedKeys.close();
            statement1.close();
            
           
               
              //Commentato per eviatare l'errore di deadlock
              //<------------------------------------
                try{
                    executeQueryWithRetry( connection2, IDZBLocalization,R.IDBlackBox,0);
                }catch(SQLException s){
                        logger.error("Error:executeQueryWithRetry - RETRY ",s);
                }
                 
            
            
            UpdateZBRecordStato(connection, R.IDRec, 1);
        }
        return 0;
    }
    long InsertZBRecordRT(Connection connection, ZBRecord R) throws MySQLTransactionRollbackException,SQLException {
        
        //logger.debug("Starting insert.....InsertZBRecordRT");
        String QueryString;
        for (int i = 0; i < R.RecRT.NumTracciamenti; i++) {
            PreparedStatement statement1, statement2;
            ResultSet rs;
            QueryString = "select IDZBLocalization,IDBlackBox, BTimeStamp from ZBLocalization where IDBlackBox=? and BTimeStamp=? limit 1";
            statement1 = connection.prepareStatement(QueryString);
            statement1.setLong(1, R.IDBlackBox);
            statement1.setTimestamp(2, R.RecRT.T[i].Data);
            rs = statement1.executeQuery();
            if (rs.next()) {
                
                //----------------------------
                // PER TEST commento 
                //----------------------------
                logger.debug("Record RT IDZBLocalization=" + rs.getLong("IDZBLocalization") + " gia presente");
                rs.close();
                statement1.close();
                UpdateZBRecordStato(connection, R.IDRec, 3);
                continue;
                //----------------------------
                // FINE PER TEST 
                //----------------------------
            }
            rs.close();
            statement1.close();
            System.out.println("setTimestamp "+R.RecRT.T[i].Data);
            QueryString = "INSERT INTO ZBLocalization (IDBlackBox, BLat, BLong, BTimeStamp,FuelLevel, StatoZB,IDRecord,ValidGPS, QualityGPS,ReceiveTimeStamp) VALUES ("
                    + " ?, ?, ?, ?,?,?,?,?,?,CURRENT_TIMESTAMP)";
            statement1 = connection.prepareStatement(QueryString, Statement.RETURN_GENERATED_KEYS);
            statement1.setLong(1, R.IDBlackBox);
            statement1.setDouble(2, R.RecRT.T[i].Lat);
            statement1.setDouble(3, R.RecRT.T[i].Long);
            statement1.setTimestamp(4, R.RecRT.T[i].Data);
//            SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
//            statement1.setString(4, formatter.format(R.RecRT.T[i].Data));
//            statement1.setLong(4, (long)((R.RecRT.T[i].Data.getTime())/1000));
            statement1.setInt(5, R.RecRT.Fuel);
            statement1.setInt(6, R.RecRT.T[i].StatoZB);
            statement1.setLong(7, R.IDRec);
            statement1.setInt(8, R.RecRT.T[i].ValidGPS); //ValidGPS (Spare Byte 3 - Primo Bit) - Record RealTime
            statement1.setInt(9, R.RecRT.T[i].QualityGPS); //QualityGPS0 - Record RealTime
//            statement1.setTimestamp(10, new java.sql.Timestamp((new java.util.Date()).getTime())); //Receiving Time
            statement1.execute();
            ResultSet generatedKeys;
            generatedKeys = statement1.getGeneratedKeys();
            
            long IDZBLocalization = -1;
            if (generatedKeys.next()) {
                    IDZBLocalization = generatedKeys.getLong(1);
            }
            generatedKeys.close();
            statement1.close();
            
           
               
              //Commentato per eviatare l'errore di deadlock
              //<------------------------------------
              /*  try{
                    executeQueryWithRetry( connection, IDZBLocalization,R.IDBlackBox,0);
                }catch(MySQLTransactionRollbackException s){
                        throw s;
                }
              */   
            
            
            UpdateZBRecordStato(connection, R.IDRec, 1);
            
            return IDZBLocalization;
        }
        return 0;
    }
    
    
     /**
     * Inserisce un record Realtime R ricevuto dalla ZBox nel DB
     *
     * @param R
     * @return
     * @throws SQLException
     */
    int InsertZBRecordRT_OLD(Connection connection, ZBRecord R) throws SQLException {
        
        //logger.debug("Starting insert.....InsertZBRecordRT");
        String QueryString;
        for (int i = 0; i < R.RecRT.NumTracciamenti; i++) {
            PreparedStatement statement1, statement2;
            ResultSet rs;
            QueryString = "select IDZBLocalization,IDBlackBox, BTimeStamp from ZBLocalization where IDBlackBox=? and BTimeStamp=? limit 1";
            statement1 = connection.prepareStatement(QueryString);
            statement1.setLong(1, R.IDBlackBox);
            statement1.setTimestamp(2, R.RecRT.T[i].Data);
            rs = statement1.executeQuery();
            if (rs.next()) {
                
                //----------------------------
                // PER TEST commento 
                //----------------------------
                //logger.debug("Record RT IDZBLocalization=" + rs.getLong("IDZBLocalization") + " gia presente");
                //rs.close();
                //statement1.close();
                //UpdateZBRecordStato(connection, R.IDRec, 3);
                //continue;
                //----------------------------
                // FINE PER TEST 
                //----------------------------
            }
            rs.close();
            statement1.close();
            System.out.println("setTimestamp "+R.RecRT.T[i].Data);
            QueryString = "INSERT INTO ZBLocalization (IDBlackBox, BLat, BLong, BTimeStamp,FuelLevel, StatoZB,IDRecord,ValidGPS, QualityGPS,ReceiveTimeStamp) VALUES ("
                    + " ?, ?, ?, ?,?,?,?,?,?,CURRENT_TIMESTAMP)";
            statement1 = connection.prepareStatement(QueryString, Statement.RETURN_GENERATED_KEYS);
            statement1.setLong(1, R.IDBlackBox);
            statement1.setDouble(2, R.RecRT.T[i].Lat);
            statement1.setDouble(3, R.RecRT.T[i].Long);
            statement1.setTimestamp(4, R.RecRT.T[i].Data);
//            SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
//            statement1.setString(4, formatter.format(R.RecRT.T[i].Data));
//            statement1.setLong(4, (long)((R.RecRT.T[i].Data.getTime())/1000));
            statement1.setInt(5, R.RecRT.Fuel);
            statement1.setInt(6, R.RecRT.T[i].StatoZB);
            statement1.setLong(7, R.IDRec);
            statement1.setInt(8, R.RecRT.T[i].ValidGPS); //ValidGPS (Spare Byte 3 - Primo Bit) - Record RealTime
            statement1.setInt(9, R.RecRT.T[i].QualityGPS); //QualityGPS0 - Record RealTime
//            statement1.setTimestamp(10, new java.sql.Timestamp((new java.util.Date()).getTime())); //Receiving Time
            statement1.execute();
            ResultSet generatedKeys;
            generatedKeys = statement1.getGeneratedKeys();
            
            
           
            
            if (generatedKeys.next()) {
                long IDZBLocalization = generatedKeys.getLong(1);
                generatedKeys.close();
                try{
                    executeQueryWithRetry( connection, IDZBLocalization,R.IDBlackBox,0);
                }catch(SQLException s){
                        logger.error("Error:executeQueryWithRetry - RETRY ",s);
                }
                //generatedKeys.close();
                /*try{
                    
                    QueryString = "UPDATE BlackBox SET LastIDZBLocalization=? WHERE IDBlackBox=? ";
                    statement2 = connection.prepareStatement(QueryString);
                    statement2.setLong(1, IDZBLocalization);
                    statement2.setLong(2, R.IDBlackBox);
                    statement2.execute();
                    statement2.close();
                    logger.debug("Record RT IDZBLocalization=" + IDZBLocalization + " inserito");
                    //generatedKeys.close();
                    
                }catch(MySQLTransactionRollbackException t){
                    
                    //----------------------------------------
                    //PER MYSQL PROVO UN RETRY
                    //----------------------------------------
                    try{
                        //long IDZBLocalization = generatedKeys.getLong(1);
                        //generatedKeys.close();
                        QueryString = "UPDATE BlackBox SET LastIDZBLocalization=? WHERE IDBlackBox=? ";
                        statement2 = connection.prepareStatement(QueryString);
                        statement2.setLong(1, IDZBLocalization);
                        statement2.setLong(2, R.IDBlackBox);
                        statement2.execute();
                        statement2.close();
                        logger.debug("Record RT IDZBLocalization=" + IDZBLocalization + " inserito");
                        //generatedKeys.close();
                    }catch(SQLException s){
                        logger.error("Error:InsertZBRecordRT - RETRY ",s);
                    }
                    
                }*/
            }
            statement1.close();
            UpdateZBRecordStato(connection, R.IDRec, 1);
        }
        return 0;
    }
    
    public void executeQueryWithRetry(Connection connection,long IDZBLocalization,long idBlackBox,int retry) throws SQLException, MySQLTransactionRollbackException{
        String QueryString = "UPDATE BlackBox SET LastIDZBLocalization=? WHERE IDBlackBox=? ";
        try{
            PreparedStatement statement2 = connection.prepareStatement(QueryString);
            statement2.setLong(1, IDZBLocalization);
            statement2.setLong(2, idBlackBox);
            statement2.execute();
            statement2.close();
        }catch(MySQLTransactionRollbackException t){
            /*try {
                Thread.sleep(300);
            } catch (InterruptedException ex) {
                Logger.getLogger(ZBRecordsDAO.class.getName()).log(Level.SEVERE, null, ex);
            }
            retry++;
            if (retry<5)
                executeQueryWithRetry(connection,IDZBLocalization,idBlackBox, retry);
            else 
            */
            throw t;
        }
    }

    /**
     * Inserisce un record Evento R ricevuto dalla ZBox nel DB
     * 
     * Modifica per ritornare il campo auto incrementale della tabella 
     *
     * @param R
     * @return
     * @throws SQLException
     */ 
    public long InsertZBRecordE(Connection connection, ZBRecord R) throws SQLException {
        //logger.debug("Starting insert.....ZBEvents");
        logger.debug(" Record Evento: " + R.RecE.E[0].EventDescr);
        String QueryString;
        PreparedStatement statement1 = null, statement2 = null;
        ResultSet rs = null;
        ResultSet generatedKeys = null;
        
        long IDZBEvents = 0; 
        
        for (int i = 0; i < R.RecE.NumEventi; i++) {
            try {
                QueryString = "select IDZBEvents,IDBlackBox, BTimeStamp,IDType,Extra from ZBEvents where IDBlackBox=? and BTimeStamp=? "
                        + " and IDType=? and Extra=? limit 1";
                statement1 = connection.prepareStatement(QueryString);

                statement1.setLong(1, R.IDBlackBox);
                statement1.setTimestamp(2, R.RecE.E[0].T.Data);
                statement1.setLong(3, R.RecE.E[0].TypeEv);
                statement1.setBytes(4, R.RecE.E[0].Extra);
                rs = statement1.executeQuery();

                if (rs.next()) {
                    logger.debug("Record E IDZBEvents=" + rs.getLong("IDZBEvents") + " gia presente");
                    //IDZBEvents = rs.getLong("IDZBEvents");
                    
                    //------------------------------
                    // PER TEST procedo sempre all'insert
                    //------------------------------
                    continue;
                    //------------------------------
                    // FINE PER TEST 
                    //------------------------------
                }
                QueryString = "INSERT INTO ZBEvents (IDBlackBox,IDType, BLat, BLong, BTimeStamp,Extra,IDRecord,ReceiveTimeStamp) VALUES ("
                        + " ?, ?, ?, ?,?,?,?,?)";
                statement2 = connection.prepareStatement(QueryString, Statement.RETURN_GENERATED_KEYS);
                statement2.setLong(1, R.IDBlackBox);
                statement2.setInt(2, R.RecE.E[0].TypeEv);
                statement2.setDouble(3, R.RecE.E[0].T.Lat);
                statement2.setDouble(4, R.RecE.E[0].T.Long);
                statement2.setTimestamp(5, R.RecE.E[0].T.Data);
                statement2.setBytes(6, R.RecE.E[0].Extra);
                statement2.setLong(7, R.IDRec);
                
                Date currentDate = new java.util.Date();
                statement2.setTimestamp(8, new java.sql.Timestamp((currentDate).getTime())); //Receiving Time
                
                
                statement2.execute();
                
                //comfronto date
                //IF(BTimeStamp<'2012-01-01 00:00:01',ReceiveTimeStamp,BTimeStamp) BTime
                try{
                    SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
                    Date parsedDate = dateFormat.parse("2012-01-01 00:00:01");
                    Timestamp timestamp = new java.sql.Timestamp(parsedDate.getTime());
                    if ((R.RecE.E[0].T.Data).before(timestamp)) {
                        R.RecE.E[0].T.Data = new java.sql.Timestamp((currentDate).getTime());
                    }
                }catch(Exception e){//this generic but you can control another types of exception

                }
 

                

                generatedKeys = statement2.getGeneratedKeys();
                if (generatedKeys.next()) {
                    IDZBEvents = generatedKeys.getLong(1);
                    //                QueryString = "UPDATE BlackBox SET LastIDZBLocalization=? WHERE IDBlackBox=? ";
                    //                statement2 = Conn.prepareStatement(QueryString);
                    //                statement2.setLong(1,IDZBLocalization);
                    //                statement2.setLong(2,R.IDBlackBox);
                    //                statement2.execute();
                    //                statement2.close();
                    logger.debug("Record E IDZBEvents=" + IDZBEvents + " inserito");

                }

                UpdateZBRecordStato(connection, R.IDRec, 1);
            } finally {
                if (statement1 != null) {
                    statement1.close();
                }
                if (rs != null) {
                    rs.close();
                }
                if (statement2 != null) {
                    statement2.close();
                }
                if (generatedKeys != null) {
                    generatedKeys.close();
                }
            }
        }
        return IDZBEvents;
    }

    public void UpdateZBRecordStato(Connection connection, long IDRec, int Stato) throws SQLException {
        //logger.debug("Starting insert.....UpdateZBRecordStato "+Stato);
        if (IDRec != 0) {
            String QueryString = "UPDATE ZBRecords SET Stato=" + Stato + " WHERE IDRec=?";
            PreparedStatement statement1 = connection.prepareStatement(QueryString);
            statement1.setLong(1, IDRec);    //IDRec
            statement1.execute();
            statement1.close();
        }
    }
    
        /**
     * Inserisce un record Info I ricevuto dalla ZBox nel DB
     *
     * @param R
     * @return
     * @throws SQLException
     */
    int InsertZBRecordI(Connection connection, ZBRecord R) throws SQLException {
        
        logger.debug("Starting insert.....InsertZBRecordI");
        String QueryString;
        PreparedStatement statement, statement1;
        

        QueryString = "INSERT INTO ZBInfo(IDBlackBox, ErrorsNumber, MaiorVersion, MinorVersion,"
                + " DateVersion, TimeVersion, LastOperativeTime, IDRecord , BTimeStamp, HWRev,PKRandID,"
                + " NFCMaiorVersion,NFCMinorVersion) VALUES ("
                + " ?, ?, ?, ?, ?, ?, ?, ?, ?,?,?,?,?)";

        statement = connection.prepareStatement(QueryString, Statement.RETURN_GENERATED_KEYS);
        statement.setLong(1, R.IDBlackBox);
        statement.setInt(2, R.RecI.ErrorNumber);
        statement.setInt(3, R.RecI.MaiorVersion);
        statement.setInt(4, R.RecI.MinorVersion);
        statement.setString(5, R.RecI.DateVersion);
        statement.setString(6, R.RecI.TimeVersion);
        statement.setLong(7, R.RecI.LastOperativeTime);
        statement.setLong(8, R.IDRec);
        statement.setTimestamp(9, R.RecordReceivingTime);
        statement.setInt(10, R.RecI.HWRev);
        statement.setInt(11, R.RecI.PKRandID);
        statement.setInt(12, R.RecI.NFC_MaiorVersion);
        statement.setInt(13, R.RecI.NFC_MinorVersion);
        statement.execute();
        ResultSet generatedKeys;
        generatedKeys = statement.getGeneratedKeys();
        if (generatedKeys.next()) {
            
            long IDZBInfo = generatedKeys.getLong(1);
            generatedKeys.close();
            QueryString = "UPDATE BlackBox SET LastIDZBInfo=? WHERE IDBlackBox=? ";
            statement1 = connection.prepareStatement(QueryString);
            statement1.setLong(1, IDZBInfo);
            statement1.setLong(2, R.IDBlackBox);
            statement1.execute();
            statement1.close();

        }
        statement.close();

        QueryString = "UPDATE ZBRecords SET Stato=1 WHERE IDRec=?";
        statement = connection.prepareStatement(QueryString);
        statement.setLong(1, R.IDRec);    //IDRec
        statement.execute();
        statement.close();


        // verifica della versione 
        QueryString = "SELECT IDSWVersion, SWVersion, DataRilascio,MaiorVersion, MinorVersion,IDBlackBoxType FROM swversion "
                + "WHERE IDBlackBoxType=2 and MaiorVersion=? and MinorVersion=? "
                + "ORDER BY IDSWVersion desc limit 1";

        statement = connection.prepareStatement(QueryString);
        statement.setInt(1, R.RecI.MaiorVersion);
        statement.setInt(2, R.RecI.MinorVersion);
        ResultSet rs1;
        rs1 = statement.executeQuery();
        if (rs1.next()) {
            logger.debug("SW Version " + R.RecI.MaiorVersion + "." + R.RecI.MinorVersion + " ID=" + rs1.getInt("IDSWVersion"));
            QueryString = "UPDATE BlackBox SET IDSWVersion=? WHERE IdBlackBox=?";
            statement = connection.prepareStatement(QueryString);
            statement.setInt(1, rs1.getInt("IDSWVersion"));    //IDSWVersion
            statement.setLong(2, R.IDBlackBox);    //IDBlackBox
            statement.execute();
            statement.close();
        }

        return 0;
    }

    /**
     * Inserisce un record Info I ricevuto dalla ZBox nel DB
     *
     * @param R
     * @return
     * @throws SQLException
     */
    int InsertZBRecordI2(Connection connection, ZBRecord R) throws SQLException {
        
        logger.debug("Starting insert.....InsertZBRecordI2");
        if (R.RecI != null) {
            InsertZBRecordI(connection, R);
            return 0;
        }
        String QueryString;
        PreparedStatement statement;
        QueryString = "UPDATE ZBInfo SET IMEI=?, ICCID=? WHERE IDBlackBox=? AND PKRandID=? AND IMEI is null AND ICCID is null";

        statement = connection.prepareStatement(QueryString);
        statement.setString(1, R.RecI2.IMEI);
        statement.setString(2, R.RecI2.ICCID);
        statement.setLong(3, R.IDBlackBox);
        statement.setInt(4, R.RecI2.PKRandID);
        statement.execute();
        statement.close();

        QueryString = "UPDATE ZBRecords SET Stato=1 WHERE IDRec=?";
        statement = connection.prepareStatement(QueryString);
        statement.setLong(1, R.IDRec);    //IDRec
        statement.execute();
        statement.close();

        QueryString = "UPDATE BlackBox SET IMEI=? WHERE IdBlackBox=?";
        statement = connection.prepareStatement(QueryString);
        statement.setString(1, R.RecI2.IMEI);    //IMEI
        statement.setLong(2, R.IDBlackBox);    //IDBlackBox
        statement.execute();
        statement.close();

        return 0;
    }

    /**
     * Inserisce un record ConfSafe I ricevuto dalla ZBox nel DB
     *
     * @param R
     * @return
     * @throws SQLException
     */
    int InsertZBRecordI1(Connection connection, ZBRecord R) throws SQLException {
        
        logger.debug("Starting insert.....InsertZBRecordI1");
        
        String QueryString;
        PreparedStatement statement, statement1;

        if (R.RecI1.PkNum == 0) {
            int i;
            QueryString = "INSERT INTO zbconfsafe (IDBlackBox, TagD, TagT, TagS, TagA, TagB, TagW, TagH, TagE,"
                    + " SogliaSpeed, SogliaKM, NumTelMaster, NumTelSlave1 , NumTelSlave2, NumTelSlave3, NumTelSlave4,"
                    + " NumTelSlave5,NumTelSlave6,NumTelSlave7,RandAuth,PKRandID,NumTelConfig) VALUES ("
                    + " ?, ?, ?, ?, ?, ?, ?, ?, ?,?,?,?, ?, ?, ?, ?, ?, ?, ?,null,?,?)";

            statement = connection.prepareStatement(QueryString, Statement.RETURN_GENERATED_KEYS);
            statement.setLong(1, R.IDBlackBox);
            for (i = 0; i < 8; i++) {
                statement.setBoolean(2 + i, R.RecI1.Tags[i]);
            }
            for (i = 0; i < 2; i++) {
                statement.setInt(10 + i, R.RecI1.Soglia[i]);
            }

            statement.setString(12, R.RecI1.NumTelMaster);
            for (i = 0; i < 7; i++) {
                statement.setString(13 + i, R.RecI1.NumTelSlave[i]);
            }
//            statement.setInt(20,R.RecI1.RandAuth);
//            statement.setInt(20,null);
            statement.setInt(20, R.RecI1.PKRandID);
            statement.setInt(21, R.RecI1.NumTelConfig);
            statement.execute();
            ResultSet generatedKeys;
            generatedKeys = statement.getGeneratedKeys();
            if (generatedKeys.next()) {
                long IDZBConfSafe = generatedKeys.getLong(1);
                generatedKeys.close();
                QueryString = "UPDATE BlackBox SET LastIDZBConfSafe=? WHERE IDBlackBox=? ";
                statement1 = connection.prepareStatement(QueryString);
                statement1.setLong(1, IDZBConfSafe);
                statement1.setLong(2, R.IDBlackBox);
                statement1.execute();
                statement1.close();
            }
            statement.close();
        } else {
            int i;
            QueryString = "UPDATE zbconfsafe SET NumTelSlave3=?, NumTelSlave4=?,"
                    + " NumTelSlave5=?,NumTelSlave6=?,NumTelSlave7=?,RandAuth=?, NumTelConfig=? "
                    + "WHERE IDBlackBox=? AND PKRandID=? AND RandAuth is null";

            statement = connection.prepareStatement(QueryString);

            for (i = 0; i < 5; i++) {
                statement.setString(i + 1, R.RecI1.NumTelSlave[2 + i]);
            }
            statement.setLong(6, R.RecI1.RandAuth);
            statement.setInt(7, R.RecI1.NumTelConfig);
            statement.setLong(8, R.IDBlackBox);
            statement.setInt(9, R.RecI1.PKRandID);
            statement.execute();
            statement.close();
        }

        QueryString = "UPDATE ZBRecords SET Stato=1 WHERE IDRec=?";
        statement1 = connection.prepareStatement(QueryString);
        statement1.setLong(1, R.IDRec);    //IDRec
        statement1.execute();
        statement1.close();

        return 0;
    }
     
        public ZBRecord AddRecord(Connection DbRTConn, ZBRecord record, boolean CheckBefore) {
        
            //logger.debug("-----------------AddRecord---------------");
            
            ZBRecord Ret = new ZBRecord();
        Ret.IDRec = 0;
        
        byte[] backToBytes = DatatypeConverter.parseBase64Binary(record.recBase64);

        //non serve lo tolgo
        //System.arraycopy(rec, 0, Ret.Rec, 0, 48);
        //if (Conf.DisableDBWriteData == 1) {
        //    Ret.IDRec = 1;
        //    return Ret;
        //}
        PreparedStatement statement=null;
        ResultSet rs=null;
//        CheckConnection();
//        try {
//            if (DbRTConn.isClosed()) {  return Ret;  }
//        } catch (SQLException ex) {     return Ret;  }
        java.sql.Timestamp now1 = new java.sql.Timestamp((new java.util.Date()).getTime());
        String Stato = "stato 0";


        try {
//            DbRTConn.setAutoCommit(false);
            String QueryString;
            if (CheckBefore) {
                // esegue la query al DB
                Stato = "stato 1";
                QueryString = "select IDBlackBox, Record FROM ZBRecords \n\r"
                        + " WHERE IDBlackBox=? and Record=?";
                //                    + " WHERE IDBlackBox=? and Record=? and Time>?";
                try {
                    statement = DbRTConn.prepareStatement(QueryString);
                    //statement.setInt(1, ZB.IDBlackBox);
                    statement.setLong(1, record.IDBlackBox);
                    
                    //statement.setBytes(2, rec);
                    statement.setBytes(2, backToBytes);
                    //            statement.setTimestamp(3,new Timestamp((new java.util.Date()).getTime()-(30*24*60*60*1000)));
                    rs = statement.executeQuery();
                    Stato = "stato 2";

                    if (rs.next()) {
                        System.out.println("Record gia' presente");
                        Ret.IDRec = -2;
//                        DbRTConn.rollback();


                        //----------------------------
                        // PER TEST commento 
                        //----------------------------
                        //return Ret;
                        
                    }
                } finally {
                    if (rs != null) {rs.close();}
                    if (statement != null) {statement.close();}
                }
            }
            java.sql.Timestamp now2 = new java.sql.Timestamp((new java.util.Date()).getTime());
            float TimeDiff1 = ((float) (now1.getTime() - now2.getTime())) / 1000;

            Stato = "stato 3 TimeDiff1=" + TimeDiff1;
            QueryString = "INSERT INTO ZBRecords (IDBlackBox, Record, Stato, Time)"
                    + " VALUES(?,?,0,?)";
            try {
            statement = DbRTConn.prepareStatement(QueryString, Statement.RETURN_GENERATED_KEYS);
            statement.setLong(1, record.IDBlackBox);
            //statement.setBytes(2, rec);
            statement.setBytes(2, backToBytes);
            
            
            statement.setTimestamp(3, new Timestamp((new java.util.Date()).getTime()));
            statement.execute();
            Stato = "stato 4";
            Ret.IDRec = -1;
            ResultSet generatedKeys;
            generatedKeys = statement.getGeneratedKeys();
            if (generatedKeys.next()) {
                Ret.IDRec = generatedKeys.getLong(1);
                generatedKeys.close();
            }
            } finally {
                if (statement !=null) {statement.close();}
            }
//            DbRTConn.commit();
            return Ret;
        } catch (SQLException ex) {
            java.sql.Timestamp now2 = new java.sql.Timestamp((new java.util.Date()).getTime());

            float TimeDiff2 = ((float) (now1.getTime() - now2.getTime())) / 1000;
            logger.error("AddRecord Select Stato=" + Stato + " TimeDiff2=" + TimeDiff2);
            logger.error(ex);
            //Logger.getLogger(DBAdminClass.class.getName()).log(Level.SEVERE, null, ex);
//            try {
//                DbRTConn.rollback();
//            } catch (SQLException ex1) {
//                Log.WriteEx(DBAdminClass.class.getName(), ex);
//                Logger.getLogger(DBAdminClass.class.getName()).log(Level.SEVERE, null, ex1);
//            }
            return Ret;
        }

    }
}
