/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package it.acconsulting.factory;

import it.acconsulting.bean.ZBRecord;
import it.acconsulting.bean.ZBox;
import it.acconsulting.dao.ZBoxDAO;
import it.acconsulting.factory.exception.FactoryCreationException;
import java.sql.Connection;
import java.util.ArrayList;
import org.apache.log4j.Logger;

/**
 *
 * @author F.Saverio Letterese
 */
public class FactoryCreator {
    
    private ZBoxDAO dao = null;
    private Connection conn = null;
    private ZBox zBoxSel = null;
    
    private static Logger logger =  Logger.getLogger("GATEWAY");   
    
    public FactoryCreator(ZBoxDAO myDao, Connection myConn){
        dao = myDao;
        conn = myConn;
    } 
    
    public  ArrayList<GenericSender> createSender(ZBRecord myRecord, ZBox myZBox, long idZBEvent) throws FactoryCreationException{
        
        
        ArrayList<GenericSender> senders = new ArrayList<>();
        
        //-------------------------------------------
        //1)verifico se è un box ISA_IMA
        //-------------------------------------------
        int idBlackBox = myZBox.IDBlackBox;
        zBoxSel = dao.getZboxISAIMA(conn, idBlackBox);
        
        //--------------------------------------------
        // TODO: 
        //1)verificare se devo prendermi delle info da zboxSel
        // da riportare sull'oggetto Zbox
        //--------------------------------------------
        if (zBoxSel != null){
            //logger.debug("zbox - ISA IMA -");
            //return new ISA_IMA_Sender(myRecord, myZBox, idZBEvent);
            senders.add(new ISA_IMA_Sender(myRecord, myZBox, idZBEvent));
        }
        //logger.debug(">>>>>>zbox non trovata nelle tabelle ISA - IMA");
        //-------------------------------------------
        //2)verifico se è un box ISA_DMS
        //-------------------------------------------
        zBoxSel = dao.getZboxISADMS(conn, idBlackBox);
        if (zBoxSel != null){
            //logger.debug("zbox - ISA DMS -");
            //return new ISA_DMS_Sender(myRecord, myZBox, idZBEvent);
            senders.add(new ISA_DMS_Sender(myRecord, myZBox, idZBEvent));
        }
        //logger.debug(">>>>>>zbox non trovata nelle tabelle ISA - DMS");
        
        
        return senders;
    }
    
}
