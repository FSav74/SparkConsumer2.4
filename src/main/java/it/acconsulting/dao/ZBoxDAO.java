/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package it.acconsulting.dao;

import it.acconsulting.bean.ZBRecord;
import it.acconsulting.bean.ZBox;
import it.acconsulting.util.Utils;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.bouncycastle.util.encoders.Hex;


/**
 *
 * @author F.Saverio Letterese
 */
public class ZBoxDAO {
    private static org.apache.log4j.Logger logger =  org.apache.log4j.Logger.getLogger("GATEWAY");        
    
    public ZBox GetZBox(Connection conn, int IDBlackBox) {
        PreparedStatement statement;
        ResultSet rs;

        try {
            // esegue la query al DB
            ZBox ZB = null;

            String QueryString = "select IDBlackBox, BBSerial, Descr, Targa, NumTel, IDBlackBoxType , IDCarType,"
                    + " AESRootKey, AESKeyIn, AESKeyOut, IP, Port, ProtV, StatoConnessione, IDAzienda, CertidriveIDVeicolo"
                    + " from BlackBox \n\r"
                    + " WHERE IDBlackBox=" + IDBlackBox + ";";

            statement = conn.prepareStatement(QueryString);
            rs = statement.executeQuery(QueryString);
            if (rs.next()) {
                ZB = new ZBox();
                ZB.IDBlackBox = rs.getInt("IDBlackBox");
//                System.out.println("Telecom_ZB.IDBlackBox="+ZB.IDBlackBox);
                ZB.SerialN = Hex.decode(rs.getString("BBSerial"));
                ZB.Targa = rs.getString("Targa");
                ZB.IDCarType = rs.getInt("IDCarType");

                ZB.NumTel = rs.getString("NumTel");
                ZB.Descr = rs.getString("Descr");
                ZB.AESRootKey = rs.getBytes("AESRootKey");
                ZB.AESKeyIn = rs.getBytes("AESKeyIn");
                ZB.AESKeyOut = rs.getBytes("AESKeyOut");
                ZB.IDAzienda = rs.getInt("IDAzienda");

                try {
                    ZB.IP = InetAddress.getByName(rs.getString("IP"));
                } catch (UnknownHostException e) {
                    ZB.IP = null;
                }
                ZB.Port = rs.getInt("Port");
                ZB.ProtV = rs.getInt("ProtV");
                ZB.StatoConnessione = rs.getInt("StatoConnessione");
                ZB.CertidriveIDVeicolo = rs.getInt("CertidriveIDVeicolo");
                if (ZB.AESRootKey == null) {
                    ZB.AESRootKey = new byte[16];
                    java.util.Arrays.fill(ZB.AESRootKey, (byte) 0);
                }
                if (ZB.AESKeyIn == null) {
                    ZB.AESKeyIn = new byte[16];
                    java.util.Arrays.fill(ZB.AESKeyIn, (byte) 0);
                }
                if (ZB.AESKeyOut == null) {
                    ZB.AESKeyOut = new byte[16];
                    java.util.Arrays.fill(ZB.AESKeyOut, (byte) 0);
                }
            }

            rs.close();
            statement.close();
            return ZB;
        } catch (SQLException ex) {
            logger.error("Error retrieving Zbox", ex);
            return null;
        }
    }
    
    
    public ZBox getZboxISAIMA(Connection conn, int idBlackBox) {
        
        
        PreparedStatement statement;
        ResultSet rs;
        
        String ZB_DB_Table = "connbbisa_ima";
        
        ZBox ZB = null;

        try {
           

            String QueryString = "select IDBlackBox, Active, LastLocationSent, LastDriveSent, "
                    + "LastEventSent, LastCommandSent, LastTelemetrySent, InstallComplete, TemporaryDisabled "
//                                        + "from "+ZB_DB_Table+" WHERE IDBlackBox=11563";
                    + "from " + ZB_DB_Table + " WHERE Active=1 AND TemporaryDisabled = 0 AND IDBlackBox="+idBlackBox;

                   logger.debug("QUERY:"+QueryString);
            statement = conn.prepareStatement(QueryString);
            //statement.setInt(1,idBlackBox );

            rs = statement.executeQuery(QueryString);
            if (rs.next()) {

                ZB = new ZBox();
                ZB.IDBlackBox = rs.getInt("IDBlackBox");
                ZB.LastDriveSent = rs.getLong("LastDriveSent");
                ZB.LastLocationSent = rs.getLong("LastLocationSent");
                ZB.LastEventSent = rs.getLong("LastEventSent");
                ZB.LastCommandSent = rs.getLong("LastCommandSent");
                ZB.LastTelemetrySent = rs.getLong("LastTelemetrySent");
                ZB.InstallComplete = rs.getInt("InstallComplete");
            }

            rs.close();
            statement.close();
        } catch (SQLException ex) {
            logger.error("Error retrieving Zbox ISA IMA ("+ZB_DB_Table+")", ex);
            return null;
        }
        return ZB;
    }
    
     public ZBox getZboxISADMS(Connection conn, int idBlackBox) {
        Statement statement;
        ResultSet rs;

        String ZB_DB_Table = "connbbisa_dms";
    
        
        ZBox ZB = null;

        try {
            // esegue la query al DB
            statement = conn.createStatement();

            String QueryString = "select IDBlackBox, Active, LastLocationSent, LastDriveSent, "
                    + "LastEventSent, LastCommandSent, LastTelemetrySent, InstallComplete, TemporaryDisabled "
                    //                    + "from "+ZB_DB_Table+" WHERE IDBlackBox=200";
                    + "from " + ZB_DB_Table + " WHERE Active=1 AND TemporaryDisabled = 0 AND IDBlackBox="+idBlackBox;


            rs = statement.executeQuery(QueryString);
            int Counter = 0;
            if (rs.next()) {
                ZB = new ZBox();
                ZB.IDBlackBox = rs.getInt("IDBlackBox");
                ZB.LastDriveSent = rs.getLong("LastDriveSent");
                ZB.LastLocationSent = rs.getLong("LastLocationSent");
                ZB.LastEventSent = rs.getLong("LastEventSent");
                ZB.LastCommandSent = rs.getLong("LastCommandSent");
                ZB.LastTelemetrySent = rs.getLong("LastTelemetrySent");
                ZB.InstallComplete = rs.getInt("InstallComplete");
            }

            rs.close();
            statement.close();
        } catch (SQLException ex) {
            logger.error("Error retrieving Zbox ISA DMS ("+ZB_DB_Table+")", ex);
            return null;
        }

        return ZB;
    }

     public ZBox getZboxISABeMove(Connection conn, int idBlackBox) {
        
        Statement statement;
        ResultSet rs;

        String ZB_DB_Table = "connbbisa_BM";
    
        ZBox ZB = null;

        try {
            // esegue la query al DB
            statement = conn.createStatement();

            String QueryString = "select C.IDBlackBox, C.Active, C.LastLocationSent, C.LastDriveSent, "
                    + "C.LastEventSent, C.LastCommandSent, C.LastTelemetrySent, C.InstallComplete, C.TemporaryDisabled, C.FaseInst "
                    + "from " + ZB_DB_Table + " C WHERE Active=1 AND TemporaryDisabled = 0 AND IDBlackBox="+idBlackBox;;


            rs = statement.executeQuery(QueryString);
            
            if (rs.next()) {
                ZB = new ZBox();
                ZB.IDBlackBox = rs.getInt("IDBlackBox");
                ZB.LastDriveSent = rs.getLong("LastDriveSent");
                ZB.LastLocationSent = rs.getLong("LastLocationSent");
                ZB.LastEventSent = rs.getLong("LastEventSent");
                ZB.LastCommandSent = rs.getLong("LastCommandSent");
                ZB.LastTelemetrySent = rs.getLong("LastTelemetrySent");
                ZB.InstallComplete = rs.getInt("InstallComplete");
                ZB.FaseInst=rs.getInt("FaseInst"); 
               
            }

            rs.close();
            statement.close();
        } catch (SQLException ex) {
            logger.error("Error retrieving Zbox ISA Be Move ("+ZB_DB_Table+")", ex);
            return null;
        }

        return ZB;
    }

    
    
     public ZBox GetZBoxExtended(Connection conn, int IDBlackBox) {
        PreparedStatement statement;
        ResultSet rs;

        try {
            // esegue la query al DB
            ZBox ZB = null;

            String QueryString = "select IDBlackBox, BBSerial, Descr, Targa, NumTel, IDBlackBoxType , IDCarType,"
                    + " AESRootKey, AESKeyIn, AESKeyOut, IP, Port, ProtV, StatoConnessione, IDAzienda, CertidriveIDVeicolo, "
                    + " IDSWVersion,"
                    + " AutoAccFileDownload "
                    + " from BlackBox \n\r"
                    + " WHERE IDBlackBox=" + IDBlackBox + ";";

            statement = conn.prepareStatement(QueryString);
            rs = statement.executeQuery(QueryString);
            if (rs.next()) {
                ZB = new ZBox();
                ZB.IDBlackBox = rs.getInt("IDBlackBox");
//                System.out.println("Telecom_ZB.IDBlackBox="+ZB.IDBlackBox);
                ZB.SerialN = Hex.decode(rs.getString("BBSerial"));
                ZB.Targa = rs.getString("Targa");
                ZB.IDCarType = rs.getInt("IDCarType");

                ZB.NumTel = rs.getString("NumTel");
                ZB.Descr = rs.getString("Descr");
                ZB.AESRootKey = rs.getBytes("AESRootKey");
                ZB.AESKeyIn = rs.getBytes("AESKeyIn");
                ZB.AESKeyOut = rs.getBytes("AESKeyOut");
                ZB.IDAzienda = rs.getInt("IDAzienda");
                
                ZB.IDSWVersion = rs.getInt("IDSWVersion");
                ZB.AutoAccFileDownload=rs.getInt("AutoAccFileDownload");

                try {
                    ZB.IP = InetAddress.getByName(rs.getString("IP"));
                } catch (UnknownHostException e) {
                    ZB.IP = null;
                }
                ZB.Port = rs.getInt("Port");
                ZB.ProtV = rs.getInt("ProtV");
                ZB.StatoConnessione = rs.getInt("StatoConnessione");
                ZB.CertidriveIDVeicolo = rs.getInt("CertidriveIDVeicolo");
                if (ZB.AESRootKey == null) {
                    ZB.AESRootKey = new byte[16];
                    java.util.Arrays.fill(ZB.AESRootKey, (byte) 0);
                }
                if (ZB.AESKeyIn == null) {
                    ZB.AESKeyIn = new byte[16];
                    java.util.Arrays.fill(ZB.AESKeyIn, (byte) 0);
                }
                if (ZB.AESKeyOut == null) {
                    ZB.AESKeyOut = new byte[16];
                    java.util.Arrays.fill(ZB.AESKeyOut, (byte) 0);
                }
                ZB.V.Targa=ZB.Targa;
                ZB.V.Produttore="";
                ZB.V.Modello=ZB.Descr;
                ZB.V.VoucherID=""+ZB.IDBlackBox;
                ZB.D.Nome="PAOLO";
                ZB.D.Cognome="FRACASSO";
                ZB.D.Tel="00393474341587";
                ZB.D.Email="";
            }


            rs.close();
            statement.close();
            return ZB;
        } catch (SQLException ex) {
            logger.error("Error retrieving Zbox Extended.", ex);
            return null;
        }
    }
     
     
     //getZboxISABeMove
     
     
     public void commit( Connection conn) throws SQLException{
             if (conn!=null)
            conn.commit();
        }
        /* public void rollback( ) throws SQLException{
            conn.rollback();
        }*/
        
        
         public void close(Connection conn ) throws SQLException{
            if (conn!=null){
                conn.close();
                //logger.debug("Db Connection successfully Closed.");
            }else
                 logger.debug("Db Connection is null.");
        }
    
         
         
         
         
         
    public boolean manageSpecialEvents(Connection conn, ZBox ZB,ZBRecord ZBRec) throws SQLException{
       if (ZBRec.getRecordType() == ZBRecord.RecordTypes.RecordE){
           if (ZB.AutoAccFileDownload>0){          // se per la box è previsto il download automatico dei file accelerometrici
               for (int i=0;i<ZBRec.RecE.NumEventi;i++){
                   if (ZBRec.RecE.E[i].TypeEv==2){     //evento grave
                       int NumFile = (Utils.uBToI(ZBRec.RecE.E[i].Extra[1]) << 8) + Utils.uBToI(ZBRec.RecE.E[i].Extra[0]);
                       String command="GPR-Acc:"+NumFile;
                       logger.debug("ManageSpecialEvents IDBlackbox="+ZB.IDBlackBox+ " command - "+command);
                       java.sql.Timestamp Timeout= new java.sql.Timestamp(System.currentTimeMillis()+24*60*60*1000); // 24 ore di timeout
                       insertCommand(conn, ZB, command, null, Timeout);
                   }
               }
           }
       }
       return true;
    }
    
    public boolean insertCommand(Connection conn, ZBox ZB, String Command, String ReqID, Timestamp timeout) throws SQLException {
        PreparedStatement  statement;
        String QueryString = "INSERT INTO ZBCommand ( IDBlackBox, Command, Stato, ReqID, Timeout, Time) VALUES ( ?, ?, ? ,?, ?,?)"; //+
        statement = conn.prepareStatement(QueryString);
        try {
            statement.setInt(1, ZB.IDBlackBox);
            statement.setString(2, Command);
            statement.setInt(3, 0);
            statement.setString(4, ReqID);
            statement.setTimestamp(5, timeout);
            statement.setTimestamp(6, (new java.sql.Timestamp(System.currentTimeMillis())));
            
            int res = statement.executeUpdate();
            if (res==0) {
                logger.error("Errore di inserimento del COMANDO nel DB");
                return false;
            }
        }finally {
            if (statement!=null)
                statement.close();
        }
        return false;
    }
    
    
    public void getExtraInformation(Connection conn, ZBox myZbox)throws SQLException {
        
        getVoucher(conn, myZbox);
        myZbox.NumTel = getNumTel(conn, Utils.toHexString(myZbox.SerialN));
    }
    
    
    public void getVoucher(Connection conn, ZBox ZB) throws SQLException {
        PreparedStatement statement = null;
        ResultSet rs = null;
        
        String QueryString = "SELECT idVoucher,Voucher_Number,BBSerial,Cognome,Nome,Tel1,Tel2,MarcaVeicolo,ModelloVeicolo,EMail  "
                + "FROM blackbox_debug.Voucher \n\r"
                + " where Targa like ?";

        try {
            statement = conn.prepareStatement(QueryString);
            statement.setString(1, ZB.Targa);
            rs = statement.executeQuery();
            if (rs.next()) {

                ZB.V.VoucherID=""+rs.getString("Voucher_Number");
                ZB.D.Cognome= ""+rs.getString("Cognome");
                ZB.D.Nome= ""+rs.getString("Nome");
                ZB.D.Tel1= ""+rs.getString("Tel1");
                ZB.D.Tel2= ""+rs.getString("Tel2");
                ZB.V.MarcaVeicolo= " "+rs.getString("MarcaVeicolo");
                ZB.V.ModelloVeicolo= " "+rs.getString("ModelloVeicolo");
                ZB.D.Email=" "+rs.getString("EMail");
            }
            
            logger.error(">>>1email:"+ZB.D.Email);
        }finally{
            if (rs!=null)
                rs.close();
            if (statement!=null)
                statement.close();

        }
        return;
    }
     
     public String getNumTel(Connection conn, String SNBB) throws SQLException {
        PreparedStatement statement = null;
        ResultSet rs = null;
        
        String NumTel="";

        String QueryString = "SELECT * FROM blackbox_debug.sim \n\r"
                + " where SN like ?";

        try{
            statement = conn.prepareStatement(QueryString);
            statement.setString(1, SNBB);
            rs = statement.executeQuery();
            if (rs.next()) {
                NumTel=rs.getString("Tel");
            }

        }finally{
            if (rs!=null)
                rs.close();
            if (statement!=null)
                statement.close();

        }
        return NumTel;
    }
}
