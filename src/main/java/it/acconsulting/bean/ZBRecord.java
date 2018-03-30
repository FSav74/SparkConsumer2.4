/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package it.acconsulting.bean;

import java.io.Serializable;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.TimeZone;



/**
 *
 * @author Luca
 */
public class ZBRecord implements Serializable{

    public long IDBlackBox = -1;
    public long IDRec = -1;
    public Timestamp RecordReceivingTime;
    
    //dove ho salvato zbrecord in una stringa base 64 
    public String recBase64 = null;
    

    public class Tracciamento implements Serializable{

        public Timestamp Data;
        public double Lat;
        public double Long;
        public int FuelLevel;
        public int StatoZB;
        public int ValidGPS = -1;
        public int QualityGPS = -1;
    }

    public class Evento implements Serializable{

        /* TypeEv: 0 - SOS
         *          1 - ACC lieve
         *          2 - ACC Grave
         *          6 - Main Supply Off
         *          7 - Main Supply On
         */
        public int TypeEv;
        public String EventDescr;

        public Evento() {
        }
        public Tracciamento T = new Tracciamento();
        public byte[] Extra = new byte[10];

/*        public String getString() {
            SimpleDateFormat FullDateFormat = new SimpleDateFormat("yyyyMMddHHmmss");
            FullDateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));

            String ret = "";
            ret += IDBlackBox + ",";
            ret += 0 + ",";
            ret += T.Lat + ",";
            ret += T.Long + ",";
            ret += FullDateFormat.format(T.Data) + ",";
            ret += TypeEv + ",";
            ret += new String(Hex.encode(Extra));

            return ret;
        }
*/
    }

    public class TypeRecordRT implements Serializable{

        public int StatoZB = -1;
        public int Fuel = -1;
        public int NumTracciamenti;
        public Tracciamento T[] = new Tracciamento[3];

        public String getString() {
            SimpleDateFormat FullDateFormat = new SimpleDateFormat("yyyyMMddHHmmss");
            FullDateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));

            String ret = "";
            ret += IDBlackBox + ",";
            ret += 0 + ",";
            ret += T[0].Lat + ",";
            ret += T[0].Long + ",";
            ret += FullDateFormat.format(T[0].Data) + ",";
            ret += 0 + ",";
            ret += 0 + ",";
            ret += 0 + ",";
            ret += 0 + ",";
            ret += T[0].StatoZB + ",";
            ret += T[0].ValidGPS;


            return ret;
        }
    }

    public class TypeRecordZ implements Serializable{
    }

    public class TypeRecordY implements Serializable{
    }

    public class TypeRecordX implements Serializable{
    }

    public class TypeRecordI implements Serializable{

        public int ErrorNumber;
        public int MaiorVersion;
        public int MinorVersion;
        public String DateVersion = "";
        public String TimeVersion = "";
        public long LastOperativeTime;
        public int HWRev;
        public int NFC_MaiorVersion;
        public int NFC_MinorVersion;
        Timestamp BTimeStamp;
        public int PKRandID;
    }

    public class TypeRecordI2 implements Serializable{

        public String IMEI = "";
        public String ICCID = "";
        public int PKRandID;
    }

    public class TypeRecordI1 implements Serializable{

        public boolean[] Tags = new boolean[8];
        public int[] Soglia = new int[2];
        public String NumTelMaster = "";
        public String[] NumTelSlave = new String[7];
        public int NumTelConfig = 255;
        public long RandAuth;
        public int PkNum;
        public int PKRandID;
    }

    public class TypeRecordT implements Serializable{
    }

    public class TypeRecordE implements Serializable{

        public int NumEventi;
        public Evento E[] = new Evento[2];
    }

    public enum RecordTypes {

        RecordRT, RecordZ, RecordY, RecordX, RecordI, RecordT, RecordE
    };
    public int RecordSubType = 0;
    RecordTypes RecordType;

    public RecordTypes getRecordType() {
        return RecordType;
    }
    public TypeRecordRT RecRT;
    public TypeRecordE RecE;
    public TypeRecordI RecI;
    public TypeRecordI2 RecI2;
    public TypeRecordI1 RecI1;

   
    public long getIDBlackBox(){
        return IDBlackBox;
    }
    
    
}
