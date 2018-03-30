/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package it.acconsulting.dao;

import it.acconsulting.bean.EventRecord;
import it.acconsulting.bean.ZBox;
import it.acconsulting.conf.ConfigurationException;
import it.acconsulting.conf.ConfigurationProperty;
import it.acconsulting.util.Utils;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;

/**
 *
 * @author Admin
 */
public class ZBFileuploadDAO {
    
    public String downloadAccFile(Connection conn, ZBox zb, EventRecord ER) throws SQLException, FileNotFoundException, IOException, ConfigurationException
    {
        String FileName="";
        
        //identifica nome del file 
        int IDFile=Utils.uBToI(ER.EventInfo[0])+(Utils.uBToI(ER.EventInfo[1])<<8);
        String FilenameConf="ACC_"+String.format("%04d",IDFile);
        
        
        // cerca la presenza del file da scaricare
        PreparedStatement statement=null;
        ResultSet rs=null;
        String QueryString = "SELECT if (substring(U.Filename,4,3)=\"acc\",substring(U.Filename,8,8), substring(U.Filename,9,8)) N1,U.* "+
                " FROM blackbox_debug.zbfileupload U where IDBlackBox="+zb.IDBlackBox
                + " AND FileType=1 and Stato=1 "
                + " AND if (substring(U.Filename,4,3)=\"acc\",substring(U.Filename,8,8), substring(U.Filename,9,8))=\""+FilenameConf+"\" "
                + " order by FileTimestamp desc limit 1";
        try {
            statement = conn.prepareStatement(QueryString);
//            statement.setString(1, FilenameConf);
            rs = statement.executeQuery(QueryString);
            if (!rs.next()) {  return "";                  }
            
            // calcola il nome del file
            SimpleDateFormat FullDateFormat = new SimpleDateFormat("yyyyMMddHHmmss");
            java.sql.Timestamp Ts=rs.getTimestamp("FileTimestamp");
            String OutputFileName="ISA_Accelerometric_"+String.format("%04d",IDFile)+"_"+ER.IDObu+"_"+FullDateFormat.format(rs.getTimestamp("FileTimestamp"))+".raw";
            
            
            String path = ConfigurationProperty.ISTANCE.getProperty(ConfigurationProperty.LONG_PATH_DWH);
            String DestFullPath=path+OutputFileName;

            // salva il file       
            OutputStream output1;
            output1 = new FileOutputStream(DestFullPath);
            
            output1.write(rs.getBytes("FileData"));
            
            output1.close();
            
            FileName=DestFullPath;            
        }finally {
            if(statement!= null) { statement.close();      }
            if (rs != null)     {  rs.close();          }
        }
        
        return FileName;
    }


    
}
