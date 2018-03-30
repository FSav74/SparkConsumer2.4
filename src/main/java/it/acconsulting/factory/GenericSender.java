/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package it.acconsulting.factory;

import it.acconsulting.bean.EventRecord;
import it.acconsulting.bean.ZBRecord;
import it.acconsulting.bean.ZBox;
import it.acconsulting.factory.exception.SenderException;
import it.acconsulting.util.Utils;

/**
 *
 * @author F.Saverio Letterese
 */
public abstract class GenericSender {
    
    private ZBRecord record;
    private ZBox zBox;
    private EventRecord eventRecordToSend;
    
    public GenericSender(ZBRecord myRecord, ZBox myZBox, long idZBEvent){
        record = myRecord;
        zBox = myZBox;  
        eventRecordToSend = new EventRecord();
        //creo l'oggetto da inviare
        eventRecordToSend.IDGuida = -1;
        eventRecordToSend.IDZBEvents=idZBEvent;
        eventRecordToSend.IDObu=Utils.toHexString(myZBox.SerialN);
        //ER.IDObu = Utils.toHexString(zb.SerialN);

        eventRecordToSend.Lat = record.RecE.E[0].T.Lat;
        eventRecordToSend.Long = record.RecE.E[0].T.Long;

        eventRecordToSend.type = record.RecE.E[0].TypeEv;
        
        //data IF(BTimeStamp<'2012-01-01 00:00:01',ReceiveTimeStamp,BTimeStamp) BTime
        
        eventRecordToSend.Tempo = record.RecE.E[0].T.Data;
        eventRecordToSend.EventInfo = record.RecE.E[0].Extra;
    }
    
    
    public abstract void send() throws SenderException;

    
    
    public ZBRecord getRecord() {
        return record;
    }

    public void setRecord(ZBRecord record) {
        this.record = record;
    }

    public ZBox getzBox() {
        return zBox;
    }

    public void setzBox(ZBox zBox) {
        this.zBox = zBox;
    }

    public EventRecord getEventRecordToSend() {
        return eventRecordToSend;
    }

    public void setEventRecordToSend(EventRecord eventRecordToSend) {
        this.eventRecordToSend = eventRecordToSend;
    }
    
    
}
