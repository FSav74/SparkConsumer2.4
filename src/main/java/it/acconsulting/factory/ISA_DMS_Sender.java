/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package it.acconsulting.factory;

import it.acconsulting.bean.EventRecord;
import it.acconsulting.bean.ZBRecord;
import it.acconsulting.bean.ZBox;
import it.acconsulting.conf.ConfigurationException;
import it.acconsulting.conf.ConfigurationProperty;
import it.acconsulting.factory.exception.SenderException;
import it.acconsulting.util.Utils;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.text.SimpleDateFormat;
import java.util.Locale;
import java.util.TimeZone;
import org.apache.log4j.Logger;

/**
 *
 * @author F.Saverio Letterese
 */
public class ISA_DMS_Sender extends GenericSender{
    
    private static Logger logger =  Logger.getLogger("GATEWAY2");   

    public ISA_DMS_Sender(ZBRecord myRecord, ZBox myZBox, long idZBEvent){      
        super( myRecord, myZBox, idZBEvent);
    }
    
    @Override
    public void send() throws SenderException {
        
        EventRecord eventRecord = getEventRecordToSend();
        ZBox zBox = getzBox();
        int result = 0;
        //logger.debug("Procedo l'invio per ISA - DMS...event type:"+eventRecord.type);
        try{
            if (eventRecord.type == 0 ) {    
                //-------------------------
                // SOS
                //--------------------------
                logger.debug("Procedo l'invio PER DMS: SOS per zbox:"+zBox.IDBlackBox);
                result = SendEventDMS( eventRecord, zBox);
                       /* if (SendEventDMS( ER, zb)>0) {     // se l'evento non e' stato accettato
                            zb.LastEventSent=LastEvent;
                        } else {
                            System.out.println("Evento Accettato");
                            records.add(ER.getString());
                        }*/
                if(result==0) logger.debug("Evento inviato con successo a DMS");
                else logger.error("Evento NON inviato a DMS per zbox:"+zBox.IDBlackBox);
            }

            else if (eventRecord.type == 2 ) {  
                //----------------------
                // URTO
                //----------------------
                logger.debug("Procedo l'invio PER DMS: URTO per zbox:"+zBox.IDBlackBox);
                result = SendEventDMS( eventRecord, zBox);
                /*
                 if (ER.type == 2 ) {    // URTO
                        if (SendEventDMS( ER, zb)>0) {     // se l'evento non ? stato accettato
                            zb.LastEventSent=LastEvent;
                        } else {
                            System.out.println("Evento Accettato");
                            records.add(ER.getString());
                        }
                    }
                */
                if(result==0) logger.debug("Evento inviato con successo a DMS");
                else logger.error("Evento NON inviato a DMS per zbox:"+zBox.IDBlackBox);

            }         
        }catch(Exception e){
            throw new SenderException("Errore invio messaggio PER DMS",e);
        }
               

    }
    
     /**
     * 
     * @param SendEventDMS
     * @param query
     * @return 0 se la comunicazione ? andata a buon fine 
     *          >0 se le comunicazione non ? andata ab uon fine
     * @throws ProtocolException
     * @throws IOException 
     */
    public int SendEventDMS(EventRecord Ev, ZBox Zb) throws ConfigurationException{
        
        int RetValue = 1; 
        String urlDms = null;
        String apiKey = null;
        String maxRetryS = null;
        String timeOutS = null;
        String retryTimeS = null;
        urlDms = ConfigurationProperty.ISTANCE.getProperty(ConfigurationProperty.HTTP_ISA_DMS_URL);
        apiKey = ConfigurationProperty.ISTANCE.getProperty(ConfigurationProperty.API_KEY);
        maxRetryS = ConfigurationProperty.ISTANCE.getProperty(ConfigurationProperty.MAXNUMBER_RETRY_HTTP_ISA_DMS);
        timeOutS = ConfigurationProperty.ISTANCE.getProperty(ConfigurationProperty.TIMEOUT_HTTP_ISA_DMS);
        retryTimeS = ConfigurationProperty.ISTANCE.getProperty(ConfigurationProperty.RETRY_DELAY_HTTP_ISA_DMS);
        
        int maxRetry, timeOut, retryTime  = 0;
        try{
            maxRetry = Integer.parseInt(maxRetryS);
            timeOut = Integer.parseInt(timeOutS);
            retryTime = Integer.parseInt(retryTimeS);
        }catch (NumberFormatException n){
            logger.error("Errate configurazione per i parametri della connessione http. Verificare il file di proeprties. Usati valori default");
            maxRetry = 3;
            timeOut = 3000;
            retryTime = 10000;
        }        
        
        int attempt = 1;
        try {
            
            //------------------------------------------------------------------
            // il ciclo continua SE 
            // l'invio non ha avuto successo (ovvero RetValue <> 0) e 
            // non ho raggiunto il massimo numero di tentativi
            //------------------------------------------------------------------
            while( (maxRetry >= attempt)&&(RetValue!=0) ){
                logger.debug(">>>>>>DMS Attempt number :"+attempt);
                SimpleDateFormat FullDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss"); // 
                FullDateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));        // dati forniti in UTC???

                double BLat=Ev.Lat;
                double BLong=Ev.Long;
                String Lat=String.format(Locale.ENGLISH, "%.4f", BLat);
                String Long=String.format(Locale.ENGLISH, "%.4f", BLong);

                int Acc = (Utils.uBToI(Ev.EventInfo[3]) << 8) + Utils.uBToI(Ev.EventInfo[2]);
                if (Acc > 0xF000) {
                    Acc = -(0xFFFF - Acc);
                }
                float AccTot = ((float) Acc) / 1000;

                /*String input = GenStringDMS(Ev.IDZBEvents, FullDateFormat.format(Ev.Tempo), ""+Ev.type, ""+AccTot, 
                        Zb.Targa, Utils.toHexString(Zb.SerialN),Zb.NumTel.replace("+","00"), Zb.D.Nome+" "+Zb.D.Cognome, Zb.V.VoucherID, 
                        Lat, Long);
                */
                 String NumTel="";//GetNumTel(connBB, Utils.toHexString(Zb.SerialN));
                //Zb.NumTel=NumTel;
                //GetVoucher(connBB, Zb);

                String input = GenStringDMS(Ev.IDZBEvents, FullDateFormat.format(Ev.Tempo), ""+Ev.type, ""+AccTot, 
                    Zb.Targa, Utils.toHexString(Zb.SerialN),""+Utils.formatTel(Zb.NumTel), ""+Utils.formatTel(Zb.D.Tel1),
                    Utils.checkNull(Zb.D.Nome)+" "+Utils.checkNull(Zb.D.Cognome), Utils.checkNull(Zb.V.VoucherID), 
                    Lat, Long);


                String fullUrl = null;
                fullUrl = urlDms+"/"+input;
                fullUrl = fullUrl.replaceAll(" ", "%20");


                URL url1 = new URL(fullUrl);

                logger.debug("REQUEST:"+fullUrl);

                HttpURLConnection conn = (HttpURLConnection) url1.openConnection();

                conn.setRequestMethod("POST");
                conn.setRequestProperty("Content-Type", "application/json");
    //            conn.setRequestProperty("X-API-KEY", "4741EAB8B98658B9F253199BE9BBA");

                conn.setRequestProperty("X-API-KEY", apiKey);
                //conn.setReadTimeout(timeOut);
                conn.setConnectTimeout(timeOut);
                if ( (conn.getResponseCode() != HttpURLConnection.HTTP_CREATED) && (conn.getResponseCode() != HttpURLConnection.HTTP_OK) ){
                        //throw new RuntimeException("Failed : HTTP error code : "
                        //        + conn.getResponseCode());
                        logger.error("Failed : HTTP error code : "+ conn.getResponseCode()+" - attempt number:"+attempt);
                        conn.disconnect();
                        attempt++;
                        Thread.sleep(retryTime);
                        continue;
                }

                BufferedReader br = new BufferedReader(new InputStreamReader(
                                (conn.getInputStream())));

                String output;
                //System.out.println("Output from Server .... \n");
                while ((output = br.readLine()) != null) {
                    logger.debug("RESPONSE:"+output);
                    if (output.contains("\"message\":\"OK\"")) {
                        RetValue=0;
                        break;
                    }
                }
                
                if (RetValue!=0){
                    //la risposta non era corretta.....
                    logger.error("La risposta del Server non è quella desiderata.");
                     Thread.sleep(retryTime);
                }
                attempt++;
                conn.disconnect();
                
            }//fine ciclo while(shoulRetry){
        } catch (MalformedURLException e) {
              logger.error("Errore invio verso DMS:"+e.toString(),e);
              RetValue=2;
        } catch (IOException e) {
              logger.error("Errore invio verso DMS:"+e.toString(),e);
              RetValue=3;
        } catch (Exception e ) {
              logger.error("Errore invio verso DMS:"+e.toString(),e);
              RetValue=4;
        }
        return RetValue;
    }
    
    /**
     * 
     * @param SendEventDMS
     * @param query
     * @return 0 se la comunicazione ? andata a buon fine 
     *          >0 se le comunicazione non ? andata ab uon fine
     * @throws ProtocolException
     * @throws IOException 
     */
    public int SendEventDMSOLD(EventRecord Ev, ZBox Zb) throws ConfigurationException{
        
        int RetValue = 1; 
        String urlDms = null;
        String apiKey = null;
        urlDms = ConfigurationProperty.ISTANCE.getProperty(ConfigurationProperty.HTTP_ISA_DMS_URL);
        apiKey = ConfigurationProperty.ISTANCE.getProperty(ConfigurationProperty.API_KEY);
        
        try {
            SimpleDateFormat FullDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss"); // 
            FullDateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));        // dati forniti in UTC???
            
            double BLat=Ev.Lat;
            double BLong=Ev.Long;
            String Lat=String.format(Locale.ENGLISH, "%.4f", BLat);
            String Long=String.format(Locale.ENGLISH, "%.4f", BLong);
            
            int Acc = (Utils.uBToI(Ev.EventInfo[3]) << 8) + Utils.uBToI(Ev.EventInfo[2]);
            if (Acc > 0xF000) {
                Acc = -(0xFFFF - Acc);
            }
            float AccTot = ((float) Acc) / 1000;

            /*String input = GenStringDMS(Ev.IDZBEvents, FullDateFormat.format(Ev.Tempo), ""+Ev.type, ""+AccTot, 
                    Zb.Targa, Utils.toHexString(Zb.SerialN),Zb.NumTel.replace("+","00"), Zb.D.Nome+" "+Zb.D.Cognome, Zb.V.VoucherID, 
                    Lat, Long);
            */
            
            String NumTel="";//GetNumTel(connBB, Utils.toHexString(Zb.SerialN));
            //Zb.NumTel=NumTel;
            //GetVoucher(connBB, Zb);

            String input = GenStringDMS(Ev.IDZBEvents, FullDateFormat.format(Ev.Tempo), ""+Ev.type, ""+AccTot, 
                    Zb.Targa, Utils.toHexString(Zb.SerialN),""+Utils.formatTel(Zb.NumTel), ""+Utils.formatTel(Zb.D.Tel1),
                    Utils.checkNull(Zb.D.Nome)+" "+Utils.checkNull(Zb.D.Cognome), Utils.checkNull(Zb.V.VoucherID), 
                    Lat, Long);
            
            
            String fullUrl = null;
            fullUrl = urlDms+"/"+input;
            fullUrl = fullUrl.replaceAll(" ", "%20");


            URL url1 = new URL(fullUrl);

            logger.debug("URL DMS :"+fullUrl);

            HttpURLConnection conn = (HttpURLConnection) url1.openConnection();

            conn.setRequestMethod("POST");
            conn.setRequestProperty("Content-Type", "application/json");
//            conn.setRequestProperty("X-API-KEY", "4741EAB8B98658B9F253199BE9BBA");
            
            conn.setRequestProperty("X-API-KEY", apiKey);

            if ( (conn.getResponseCode() != HttpURLConnection.HTTP_CREATED) && (conn.getResponseCode() != HttpURLConnection.HTTP_OK) ){
                    throw new RuntimeException("Failed : HTTP error code : "
                            + conn.getResponseCode());
            }

            BufferedReader br = new BufferedReader(new InputStreamReader(
                            (conn.getInputStream())));

            String output;
            System.out.println("Output from Server .... \n");
            while ((output = br.readLine()) != null) {
                System.out.println(output);
                if (output.contains("\"message\":\"OK\"")) {
                    RetValue=0;
                    break;
                }
            }

            conn.disconnect();
        } catch (MalformedURLException e) {
              e.printStackTrace();
              RetValue=2;
        } catch (IOException e) {
              e.printStackTrace();
              RetValue=3;
        } catch (Exception e ) {
              e.printStackTrace();
              RetValue=4;
        }
        return RetValue;
    }

    public static String GenStringDMSOld(long event_id, String event_date, String tipology, String event_size, 
            String car_plate, String device_sn, String device_tel, String name_surname, String insurance_number, 
            String isa_event_lat, String isa_event_long) {
        
        String input = "posteventsdevice/"+event_id+"/"+event_date+"/"+tipology+"/"+event_size
                +"/"+car_plate+"/"+device_sn+"/"+device_tel+"/"+name_surname+"/"+insurance_number
                +"/"+isa_event_lat+"/"+isa_event_long;
        
        return input;

    }
    
    public static String GenStringDMS(long event_id, String event_date, String tipology, String event_size, 
            String car_plate, String device_sn, String device_tel,String NumTelUser, String name_surname, String insurance_number, 
            String isa_event_lat, String isa_event_long) {
        
        String input = "posteventsdevice/"+event_id+"/"+event_date+"/"+tipology+"/"+event_size
                +"/"+car_plate+"/"+device_sn+"/"+device_tel+"/"+NumTelUser+"/"+name_surname+"/"+insurance_number
                +"/"+isa_event_lat+"/"+isa_event_long;
        
        return input;

    }
    
}
