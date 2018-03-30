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
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.ProtocolException;
import java.net.URL;
import java.security.GeneralSecurityException;
import java.security.cert.X509Certificate;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.TimeZone;
import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSession;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.Marshaller;
import org.apache.log4j.Logger;

/**
 *
 * @author F.Saverio Letterese
 */
public class ISA_IMA_Sender extends GenericSender{
    
    private static Logger logger =  Logger.getLogger("GATEWAY2");     
    
    public ISA_IMA_Sender(ZBRecord myRecord, ZBox myZBox, long idZBEvent){
        super( myRecord, myZBox, idZBEvent);
    }

    @Override
    public void send() throws SenderException{
        
        EventRecord record = getEventRecordToSend();
        ZBox zBox = getzBox();
        int result = 0;
        //logger.debug("Procedo l'invio per ISA - IMA...event type:"+record.type);
        
        
        
        try{
            //-----------------------------
            // URTO
            //-----------------------------
            if (record.type == 2 ) {
                initConn();
                URTO.Call Call=GenerateCallUrto(record, zBox);
                logger.debug("Procedo l'invio PER IMA: URTO per zbox:"+zBox.IDBlackBox);
                result = SendEventIMA(Call);  //>0) {     // se l'evento non ? stato accettato
                          /*      zb.LastEventSent=LastEvent;
                            } else {
                                records.add(ER.getString());
                            }*/
                if(result==0) logger.debug("Evento inviato con successo a IMA");
                else logger.error("Evento NON inviato a IMA per zbox:"+zBox.IDBlackBox);
            }
            //----------------------------------
            // SOS
            //----------------------------------
            else if (record.type == 0 ) {    
                initConn();
                SOS.Call Call=GenerateCallSOS(record, zBox);
                logger.debug("Procedo l'invio PER IMA: SOS per zbox:"+zBox.IDBlackBox);
                result = SendEventIMA(Call);  //>0 se l'evento non ? stato accettato
                
                if(result==0) logger.debug("Evento inviato con successo a IMA");
                else logger.error("Evento NON inviato a IMA per zbox:"+zBox.IDBlackBox);
                //TODO:<----------------------------------
                //TODO:<----------------------------------
                //TODO:<----------------------------------
                //Gestione errore
                
                
                   /* try {
                        if (SendEventIMA(Call)>0) {     // 
                            zb.LastEventSent=LastEvent;
                        } else {
                            records.add(ER.getString());
                        }
                    } catch (ProtocolException ex) {
                        Logger.getLogger(LogicTIMover_ISA_IMA.class.getName()).log(Level.SEVERE, null, ex);
                    } catch (IOException ex) {
                        Logger.getLogger(LogicTIMover_ISA_IMA.class.getName()).log(Level.SEVERE, null, ex);
                    }*/
              
            }
            
            
            
           
            
        }catch(Exception e){
            throw new SenderException("Errore invio messaggio PER IMA", e);
        }
                      
        
    }
    
    /**
     * 
     *  Create a trust manager that does not validate certificate chains
     * 
     */
     private void initConn() {
        
        TrustManager[] trustAllCerts = new TrustManager[] { 
            new X509TrustManager() {     
                @Override
                public java.security.cert.X509Certificate[] getAcceptedIssuers() { 
                    return new X509Certificate[0];
                } 
                @Override
                public void checkClientTrusted( 
                    java.security.cert.X509Certificate[] certs, String authType) {
                    } 
                @Override
                public void checkServerTrusted( 
                    java.security.cert.X509Certificate[] certs, String authType) {
                }
            } 
        }; 

        // Install the all-trusting trust manager
        try {
            SSLContext sc = SSLContext.getInstance("SSL"); 
            // Create empty HostnameVerifier
			HostnameVerifier hv = new HostnameVerifier() {
				public boolean verify(String arg0, SSLSession arg1) {
					return true;
				}
			};

            sc.init(null, trustAllCerts, new java.security.SecureRandom()); 
            HttpsURLConnection.setDefaultSSLSocketFactory(sc.getSocketFactory());
            HttpsURLConnection.setDefaultHostnameVerifier(hv);

        } catch (GeneralSecurityException e) {
        } 
    }

     private URTO.Call GenerateCallUrto( EventRecord ER, ZBox ZB){
        java.util.Date today = new java.util.Date();
        Timestamp EV_Time=new java.sql.Timestamp(today.getTime());
        
        SimpleDateFormat FullDateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssZ"); // ISO 8601
        FullDateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));        // dati forniti in UTC???
        
        double BLat=ER.Lat;
        double BLong=ER.Long;
        String Lat=""+Math.round(BLat*  10000000);
        String Long=""+Math.round(BLong*10000000);
        int HDOP=ER.hdop;
        
        int Acc = (Utils.uBToI(ER.EventInfo[3]) << 8) + Utils.uBToI(ER.EventInfo[2]);
        if (Acc > 0xF000) {
            Acc = -(0xFFFF - Acc);
        }
//        float AccTot = ((float) Acc) / 1000;

        
        URTO.Call C= new URTO.Call();
        
        C.setUidSupplier(""+ER.IDZBEvents);
        C.setSourcePlatformCode("GENS_ZDG");              
        C.setTargetPlatformCode("ZDG_G/FI/VE1");          
        
        C.setHardwareTimestamp(FullDateFormat.format(EV_Time));
        
        //ContractualContext
        URTO.ContractualContext Contract=new URTO.ContractualContext();
        Contract.setClientCompanyCode("ZDG");                   // Riferimento ad ISA   
        Contract.setContractualId(ZB.V.VoucherID);              // Riferimento al voucher ID
        C.setContractualContext(Contract);
        
        // Request
        URTO.EcallRequest Req=new URTO.EcallRequest();
        URTO.KeyValuePair K=new URTO.KeyValuePair();
        K.setKey("ketName");
        K.setValue("value");
        Req.getOtherInformations().add(K);
//        Req.setContextCode("");                     // Che valori dobbiamo impostare?

        // vehicle
        URTO.Vehicle V= new URTO.Vehicle();
        V.setRegistration(ZB.Targa);
//        V.setVehicleIdentification("123A12345RT654897");
        V.setMake(ZB.V.Produttore);
        V.setModel(ZB.V.Modello);
//        V.setColor("blue");
//        Req.setRequestObject(V);
//        C.setRequest(Req);
        URTO.AssistanceRequest Ass= new URTO.AssistanceRequest();
        Ass.setContextCode("VEH_GEN");
        
        URTO.VehicleAccidentContext ContOBJ=new URTO.VehicleAccidentContext();
//        ContOBJ.setRolled(Boolean.TRUE);
        ContOBJ.setCrash(Boolean.TRUE);
        ContOBJ.setDeccel(Acc);
        ContOBJ.setDeccelUnit("mG");
//        AReq.setContextData(ContOBJ);
//        Req.setAutomatic(true);
        Ass.setContextData(ContOBJ);
//        C.setRequest(Req);
        Ass.setRequestObject(V);
        C.setRequest(Ass);
        
        // Location
        URTO.LocationHeader LocH=new URTO.LocationHeader();
        LocH.setProjectionSystemCode("1");
        if (HDOP*2<=150) {                          // U se HDOP<150 metri altrimenti V
            LocH.setLocationConfidence("U");
        } else {
            LocH.setLocationConfidence("V");        
        }
        LocH.setGpsAccuracy(""+HDOP*2);                  // inserire HDOP convertito in metri
        LocH.setGpsAccuracyUnit("M");
        LocH.setAltitudeUnit("M");
        LocH.setLastLocationTimestamp(FullDateFormat.format(ER.Tempo));
        
        URTO.Location L=new URTO.Location();
        L.setLatitude(Lat);
        L.setLongitude(Long);
        L.setDirection(0);
        LocH.getLocations().add(L);
        C.setLocation(LocH);
        
        // Caller
        URTO.Caller Caller= new URTO.Caller();
        Caller.setFirstname(ZB.D.Nome);
        Caller.setName(ZB.D.Cognome);
        Caller.setPhoneNumber(ZB.D.Tel1);
        Caller.setEmail(ZB.D.Email);
        Caller.setFavoriteContactMean("TL");
        C.setCaller(Caller);
        
        // Contact
        URTO.Contact Cont= new URTO.Contact();
        Cont.setType("TL");      //TL= Phone
        Cont.setContact(ZB.NumTel);
        Cont.setUidEquipement("-");                  // Che valori dobbiamo impostare?
        C.setInitialContact(Cont);
        
        return C;
    }

    private SOS.Call GenerateCallSOS( EventRecord ER, ZBox ZB){
        java.util.Date today = new java.util.Date();
        Timestamp EV_Time=new java.sql.Timestamp(today.getTime());
        
        SimpleDateFormat FullDateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssZ"); // ISO 8601
        FullDateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));        // dati forniti in UTC???
        
        double BLat=ER.Lat;
        double BLong=ER.Long;
        String Lat=""+Math.round(BLat*  10000000);
        String Long=""+Math.round(BLong*10000000);
        int HDOP=ER.hdop;
        
        SOS.Call C= new SOS.Call();
        
        C.setUidSupplier(""+ER.IDZBEvents);
        C.setSourcePlatformCode("GENS_ZDG");              
        C.setTargetPlatformCode("ZDG_G/FI/VE1");          
        
        C.setHardwareTimestamp(FullDateFormat.format(EV_Time));
        
        //ContractualContext
        SOS.ContractualContext Contract=new SOS.ContractualContext();
        Contract.setClientCompanyCode("ZDG");
        Contract.setContractualId(ZB.V.VoucherID);              // Che valori dobbiamo impostare?
        C.setContractualContext(Contract);
        
        // Request
        SOS.EcallRequest Req=new SOS.EcallRequest();
        SOS.KeyValuePair K=new SOS.KeyValuePair();
        K.setKey("ketName");
        K.setValue("value");
        Req.getOtherInformations().add(K);
//        Req.setContextCode("");                     // Che valori dobbiamo impostare?

        // vehicle
        SOS.Vehicle V= new SOS.Vehicle();
        V.setRegistration(ZB.Targa);
//        V.setVehicleIdentification("123A12345RT654897");
        V.setMake(ZB.V.Produttore);
        V.setModel(ZB.V.Modello);
//        V.setColor("blue");
//        Req.setRequestObject(V);

//        URTO.VehicleAccidentContext ContOBJ=new URTO.VehicleAccidentContext();
//        ContOBJ.setRolled(Boolean.TRUE);
//        ContOBJ.setCrash(Boolean.TRUE);
//        ContOBJ.setDeccel(5);
////        AReq.setContextData(ContOBJ);
//        Req.setAutomatic(true);
//        Req.setContextData(ContOBJ);
//        C.setRequest(Req);
//        SOS.AssistanceRequest Req=new SOS.AssistanceRequest();
//        ContOBJ.setRolled(Boolean.TRUE);
//        ContOBJ.setCrash(Boolean.TRUE);
//        ContOBJ.setDeccel(5);
//        AReq.setContextData(ContOBJ);
//        Req.setAutomatic(true);
//        Req.setContextData(ContOBJ);
//        Req.setRequestObject((URTO.Request)ContOBJ);
        C.setRequest(Req);
        SOS.AssistanceRequest Ass= new SOS.AssistanceRequest();
        Ass.setContextCode("VEH_GEN");
        Ass.setRequestObject(V);
        C.setRequest(Ass);
        
        // Location
        SOS.LocationHeader LocH=new SOS.LocationHeader();
        LocH.setProjectionSystemCode("1");
        if (HDOP*2<=150) {                          // U se HDOP<150 metri altrimenti V
            LocH.setLocationConfidence("U");
        } else {
            LocH.setLocationConfidence("V");        
        }
        LocH.setGpsAccuracy(""+HDOP*2);                  // inserire HDOP convertito in metri
        LocH.setGpsAccuracyUnit("M");
        LocH.setAltitudeUnit("M");
        LocH.setLastLocationTimestamp(FullDateFormat.format(ER.Tempo));
        
        SOS.Location L=new SOS.Location();
        L.setLatitude(Lat);
        L.setLongitude(Long);
        L.setDirection(270);
        LocH.getLocations().add(L);
        C.setLocation(LocH);
        
        
        // Caller
        SOS.Caller Caller= new SOS.Caller();
        Caller.setFirstname(ZB.D.Nome);
        Caller.setName(ZB.D.Cognome);
        Caller.setPhoneNumber(ZB.D.Tel1);
        Caller.setEmail(ZB.D.Email);
        Caller.setFavoriteContactMean("TL");
        C.setCaller(Caller);
        
        // Contact
        SOS.Contact Cont= new SOS.Contact();
        Cont.setType("TL");      //TL= Phone
        Cont.setContact(ZB.NumTel);
        Cont.setUidEquipement("-");                  // Che valori dobbiamo impostare?
        C.setInitialContact(Cont);
        
        logger.error(">>>2email:"+Caller.getEmail());
        return C;
    }  
    
    private int SendEventIMA(Object query) throws ProtocolException, IOException, ConfigurationException {
        int RetValue = -1;
        HttpsURLConnection httpsConn;
        URL url;
        
        String httpsUrl = ConfigurationProperty.ISTANCE.getProperty(ConfigurationProperty.HTTP_ISA_IMA_URL);
        try {
            
            String maxRetryS = ConfigurationProperty.ISTANCE.getProperty(ConfigurationProperty.MAXNUMBER_RETRY_HTTP_ISA_IMA);
            String timeOutS = ConfigurationProperty.ISTANCE.getProperty(ConfigurationProperty.TIMEOUT_HTTP_ISA_IMA);
            String retryTimeS = ConfigurationProperty.ISTANCE.getProperty(ConfigurationProperty.RETRY_DELAY_HTTP_ISA_IMA);
        
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
            
            
            url = new URL(httpsUrl);
            logger.debug(">URL IMA:"+httpsUrl);
//            JAXBContext jc1 = JAXBContext.newInstance(query.getClass());
//            Marshaller marshaller1 = jc1.createMarshaller();
//            marshaller1.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, true);
//            marshaller1.setProperty(Marshaller.JAXB_SCHEMA_LOCATION, "http://not.ima.tm.fr/telematic call_fr.tm.ima.not.telematic.call.xsd ");
//            marshaller1.marshal(query, System.out);

            
            //------------------------------------------------------------------
            // il ciclo continua SE 
            // l'invio non ha avuto successo (ovvero RetValue <> 0) e 
            // non ho raggiunto il massimo numero di tentativi
            //------------------------------------------------------------------
            while( (maxRetry >= attempt)&&(RetValue!=0) ){
                logger.debug(">>>>>>IMA Attempt number :"+attempt);
                httpsConn = (HttpsURLConnection)url.openConnection();
                httpsConn.setRequestMethod("POST");
                httpsConn.setRequestProperty("Content-Type","application/xml"); 
                httpsConn.setConnectTimeout(timeOut);
                httpsConn.setDoOutput(true); 
                httpsConn.setDoInput(true); 
                
                

                DataOutputStream output = new DataOutputStream(httpsConn.getOutputStream());
    //            try {
    //                JAXBContext jc = JAXBContext.newInstance(URTO.Call.class);
                JAXBContext jc = JAXBContext.newInstance(query.getClass());
                Marshaller marshaller = jc.createMarshaller();
                marshaller.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, true);
                marshaller.setProperty(Marshaller.JAXB_SCHEMA_LOCATION, "http://not.ima.tm.fr/telematic call_fr.tm.ima.not.telematic.call.xsd ");
                marshaller.marshal(query, System.out);
                marshaller.marshal(query, output);
    //                JAXB.marshal(query, System.out);
    //                JAXB.marshal(query, output);
    //            }
    //            DataOutputStream output = new DataOutputStream(HttpsConn.getOutputStream());
    //
    //            output.write(TestString.getBytes("UTF-8"));
                
                if ( (httpsConn.getResponseCode() != httpsConn.HTTP_CREATED) && (httpsConn.getResponseCode() != httpsConn.HTTP_OK) ){
                      
                        logger.error("Failed : HTTPS error code : "+ httpsConn.getResponseCode()+" - attempt number:"+attempt);
                        httpsConn.disconnect();
                        attempt++;
                        Thread.sleep(retryTime);
                        continue;
                }
                
                DataInputStream input = new DataInputStream( httpsConn.getInputStream() ); 
                String Out="";
                for( int c = input.read(); c != -1; c = input.read() ) {
                    Out+=(char)c;
                }
                logger.debug("RESPONSE:"+ Out );
                input.close(); 

                if (Out.contains("<stateCode>0</stateCode>")) {
                    RetValue=0;
                } else {
                    RetValue=1;
                    logger.error("La risposta del Server IMA  non è quella desiderata.");
                    Thread.sleep(retryTime);
                }
                attempt++;
                httpsConn.disconnect();
                //logger.debug("\nResp Code:"+httpsConn.getResponseCode()); 
                //logger.debug("Resp Message:"+ httpsConn.getResponseMessage()); 
    
            }//while
        } catch (Exception ex) {
            logger.error("Errore invio verso IMA:"+ex.toString(),ex);
        }finally {
            
        }
        return RetValue;
    }
    

}
