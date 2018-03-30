/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package it.acconsulting.bean;

import it.acconsulting.util.Utils;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.TimeZone;

/**
 *
 * @author Admin
 */
public class EventRecord {

    public String IDObu;
    public long IDZBEvents;
    public int IDGuida;
    public double Lat;
    public double Long;
    public int hdop;
    public Timestamp Tempo;
    public int type;
    public byte[] EventInfo;
    //public Token Tk;

    String getString() {
        SimpleDateFormat FullDateFormat = new SimpleDateFormat("yyyyMMddHHmmss");
        FullDateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));

        String ret = "";
        ret += IDObu + ",";
        ret += IDGuida + ",";
        ret += Lat + ",";
        ret += Long + ",";
        if (Tempo == null) {
            Tempo = new Timestamp((new java.util.Date()).getTime());
        }

        ret += FullDateFormat.format(Tempo) + ",";
        ret += type + ",";
        System.out.println(ret);
        if (EventInfo != null) {
            ret += Utils.toHexString(EventInfo);
        }
        ret += "\n";


        return ret;
    }
}

