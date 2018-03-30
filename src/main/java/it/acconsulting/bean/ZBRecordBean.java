/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package it.acconsulting.bean;

import java.io.Serializable;

/**
 *
 * @author Admin
 */
public class ZBRecordBean implements Serializable{
    
    private String IDRec;
    private String IDBlackBox ;
    private String Record ;
    private String Stato ;
    private String Time;
                            

    public String getIDRec() {
        return IDRec;
    }

    public void setIDRec(String IDRec) {
        this.IDRec = IDRec;
    }

    public String getIDBlackBox() {
        return IDBlackBox;
    }

    public void setIDBlackBox(String IDBlackBox) {
        this.IDBlackBox = IDBlackBox;
    }

    public String getRecord() {
        return Record;
    }

    public void setRecord(String Record) {
        this.Record = Record;
    }

    public String getStato() {
        return Stato;
    }

    public void setStato(String Stato) {
        this.Stato = Stato;
    }

    public String getTime() {
        return Time;
    }

    public void setTime(String Time) {
        this.Time = Time;
    }
  
                            
    
}
