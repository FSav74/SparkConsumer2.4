/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package it.acconsulting.factory;

import it.acconsulting.bean.ZBRecord;
import it.acconsulting.bean.ZBox;
import it.acconsulting.factory.exception.SenderException;
import org.apache.log4j.Logger;

/**
 *
 * @author F.Saverio Letterese
 */
public class ISA_DWH_Sender extends GenericSender{

    private static Logger logger =  Logger.getLogger("GATEWAY");     
    
    public ISA_DWH_Sender(ZBRecord myRecord, ZBox myZBox, long idZBEvent){
        super( myRecord, myZBox, idZBEvent);
    }
    
    @Override
    public void send() throws SenderException {
        
    }
    
}
