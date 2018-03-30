/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package it.acconsulting.bean;

import it.acconsulting.util.Utils;
import java.io.Serializable;
import java.net.InetAddress;

/** 
 *
 * @author Admin
 */
public class ZBox implements Serializable{

    public int IDBlackBox;
    public byte[] SerialN = new byte[4];
    public String NumTel = "-----";
    public String Descr = "-----";
    public String Targa = "-----";
    public int IDCarType;
    public byte[] AESRootKey = new byte[16];
    public byte[] AESKeyIn = new byte[16];
    public byte[] AESKeyOut = new byte[16];
    public InetAddress IP;
    public int Port = 0;
    public int ProtV = 0;
    public int IDZBox;
    public int PackNRef = 0;
    public int StatoConnessione = 0;
    public int IDAzienda = 0;
    public boolean SubNet[];
    public int CertidriveIDVeicolo;
    public long LastLocationSent;
    public long LastDriveSent;
    public long LastEventSent;
    public long LastCommandSent;
    public long LastTelemetrySent;
    public int EmergencyTrackingTime;
    public int InstallComplete;
    public int IDSWVersion;
    
    public int FaseInst=0;

    public Vehicle V;
    public Driver D;
   
    public int AutoAccFileDownload=0;
    
    public ZBox() {
        java.util.Arrays.fill(SerialN, (byte) 0);
        java.util.Arrays.fill(AESRootKey, (byte) 0);
        java.util.Arrays.fill(AESKeyIn, (byte) 0);
        java.util.Arrays.fill(AESKeyOut, (byte) 0);
        
        V = new Vehicle();
        D = new Driver();
    }

    public long GetZBCod() {
        long Cod = (Utils.uBToL(SerialN[0]) << 24)
                + (Utils.uBToL(SerialN[1]) << 16)
                + (Utils.uBToL(SerialN[2]) << 8)
                + (Utils.uBToL(SerialN[3]));
        return Cod;
    }
}
