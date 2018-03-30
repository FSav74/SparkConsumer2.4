/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package it.acconsulting.util;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;

/**
 *
 * @author F.Saverio Letterese
 */
public class Utils {

    //-------------------------------
    //Verifica se null
    //formatta il telefono
    //-------------------------------
    public static String formatTel(String telefono){    
        if (telefono==null) return "";        
        return telefono.replace("+","00");
    }
    public static String checkNull(String input){    
        if (input==null) return "";        
        return input;
    }
    
    
    /**
     * Fast convert a byte array to a hex string with possible leading zero.
     *
     * @param b array of bytes to convert to string
     * @return hex representation, two chars per byte.
     */
    public static String toHexString(byte[] b) {
        StringBuilder sb = new StringBuilder(b.length * 2);
        for (int i = 0; i < b.length; i++) {
            // look up high nibble char
            sb.append(hexChar[(b[i] & 0xf0) >>> 4]);

            // look up low nibble char
            sb.append(hexChar[b[i] & 0x0f]);
        }
        return sb.toString();
    }
    
    public static String getStackTrace (Throwable t){
        StringWriter stringWriter = new StringWriter();
        PrintWriter  printWriter  = new PrintWriter(stringWriter);
        t.printStackTrace(printWriter);
        printWriter.close();    //surprise no IO exception here
        try {
            stringWriter.close();
        }
        catch (IOException e) {
        }
        return stringWriter.toString();
    }

    public static int uBToI(byte b) {
        return (int) b & 0xFF;
    }

    public static long uBToL(byte b) {
        return (long) b & 0xFF;
    }
    /**
     * table to convert a nibble to a hex char.
     */
    static char[] hexChar = {
        '0', '1', '2', '3',
        '4', '5', '6', '7',
        '8', '9', 'A', 'B',
        'C', 'D', 'E', 'F'};
}
