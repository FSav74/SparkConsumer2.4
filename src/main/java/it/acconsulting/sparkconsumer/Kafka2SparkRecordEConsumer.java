/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package it.acconsulting.sparkconsumer;

import com.google.gson.Gson;
import it.acconsulting.bean.ZBRecord;
import it.acconsulting.dao.ZBRecordsDAO;
import it.acconsulting.dao.ZBoxDAO;
import it.acconsulting.bean.ZBox;
import it.acconsulting.conf.ConfigurationException;
import it.acconsulting.conf.ConfigurationProperty;
import it.acconsulting.db.DBManager;
import it.acconsulting.factory.FactoryCreator;
import it.acconsulting.factory.GenericSender;
import it.acconsulting.util.Utils;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import kafka.serializer.StringDecoder;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function0;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.HasOffsetRanges;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.apache.spark.streaming.kafka.OffsetRange;
import scala.Tuple2;

/**
 *
 * @author F.Saverio Letterese
 * 
 * 
 * -- Consumer per il salvataggio Messaggi DATART di tipo RECORD E --
 * 
 * 
 * Riceve RDD di json e li converte in RDD di oggetti ZBRecord (quelli usati dal udpServer) 
 * 1)Salva gli eventi di tipo Record E 
 * 2)li invia (se di tipo 0, 2) a ima e/o isa (a seconda a chi appartenga la bb)
 * 
 * 3)Gestisce checkpoint di Spark.
 * 4)Stampa offset di Spark e il LAG di scodamento 
 * 
 */
public class Kafka2SparkRecordEConsumer {
    private static Logger logger =  Logger.getLogger("GATEWAY2");        
    
    public static void main(String[] args) {
    
        final String versione = "16";    
        java.util.Date date= new java.util.Date();
	Timestamp timestamp = new Timestamp(date.getTime());
        
        logger.info("["+timestamp+"]Starting Kafka2SparkRecordEConsumer....."+versione);
        

        if (args.length < 2) {
          System.err.println("Usage: Kafka2SparkRecordEConsumer <brokers> <topics>\n" +
              "  <brokers> is a list of one or more Kafka brokers\n" +
              "  <topics> is a list of one or more kafka topics to consume from\n\n");
          System.exit(1);
        }

        //StreamingExamples.setStreamingLogLevels();

        String brokers = args[0];
        String topics = args[1];
        
        final String maxCoreApplication; // numero di core Spark per l'applicazione
        final String checkpointFolder;   // folder Spark CheckPoint
        try {
            maxCoreApplication = ConfigurationProperty.ISTANCE.getProperty(ConfigurationProperty.MAX_CORE_APPLICATION_RECORD_E);
        } catch (ConfigurationException ex) {
            logger.error("errore configurazione MAX_CORE_APPLICATION_RECORD_E.");
            throw new  RuntimeException("Errore configurazione MAX_CORE_APPLICATION. "+ex.toString());
        }
        try {
            checkpointFolder = ConfigurationProperty.ISTANCE.getProperty(ConfigurationProperty.CHECKPOINT_PATH_2);
        } catch (ConfigurationException ex) {
            logger.error("Errore configurazione CHECKPOINT_PATH_2. ");
            throw new  RuntimeException("Errore configurazione CHECKPOINT_PATH_2. "+ex.toString());
        }

        //------------------------------------------
        // Properties consumer Kafka
        //------------------------------------------
        final HashSet<String> topicsSet = new HashSet<String>(Arrays.asList(topics.split(",")));
        final HashMap<String, String> kafkaParams = new HashMap<String, String>();
        kafkaParams.put("metadata.broker.list", brokers);
        
        
        Function0<JavaStreamingContext> createContextFunc = new Function0<JavaStreamingContext>() {
        @Override
        public JavaStreamingContext call() {
            return createContext(checkpointFolder, versione, maxCoreApplication, kafkaParams, topicsSet);
        }
        };

        JavaStreamingContext jssc2 = JavaStreamingContext.getOrCreate(checkpointFolder, createContextFunc);
        jssc2.start();
        jssc2.awaitTermination();

    }
    
    private static JavaStreamingContext createContext(String checkpointDirectory,String versione, String maxCoreApplication, HashMap<String, String> kafkaParams, HashSet<String> topicsSet){
        
        //-------------------------------------------------
        // Creazione Spark context con intervallo di 2 sec
        //-------------------------------------------------
        SparkConf sparkConf = new SparkConf().setAppName("Kafka2SparkRecordEConsumer"+versione).set("spark.cores.max", maxCoreApplication);
        JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(2));
        
        jssc.checkpoint(checkpointDirectory);
        
        JavaPairInputDStream<String, String> messages = KafkaUtils.createDirectStream(
            jssc,
            String.class,
            String.class,
            StringDecoder.class,
            StringDecoder.class,
            kafkaParams,
            topicsSet  
        );
        
        //------------------------------
        // LOG di TEST
        // Informazioni dalla coda Kafka
        //------------------------------
        displayTestOffsetKafka(messages);
        
        //--------------------------------------------------------------------------
        // Trasformo il Dstream di stringhe (Json)
        // in un Dstream di Oggetti Java ZBRecords
        //--------------------------------------------------------------------------
        JavaDStream<ZBRecord> lines2 = messages.map(new Function<Tuple2<String, String>, ZBRecord>() {
          @Override
          public ZBRecord call(Tuple2<String, String> tuple2) {

              Gson gson = new Gson();
              ZBRecord sd = gson.fromJson(tuple2._2(), ZBRecord.class);
            return sd;
          }
        });
        
        //--------------------------------------------------------------------------
        //filtrare lo stream per recuperare solo gli eventi relativi a
        // SOS e Crash
        //--------------------------------------------------------------------------
        JavaDStream<ZBRecord> filteredline = lines2.filter(new Function<ZBRecord,Boolean>() {
            @Override
            public Boolean call(ZBRecord zbrecord) throws Exception {

                if (zbrecord==null) return false;
                if (zbrecord.getRecordType().equals(ZBRecord.RecordTypes.RecordE))
                    return true;
                else return false;
            }

        });
        
        filteredline.foreachRDD(new VoidFunction<JavaRDD<ZBRecord>>() {

             @Override
            public void call(JavaRDD<ZBRecord> item) throws Exception {

                item.foreachPartition(new VoidFunction<Iterator<ZBRecord>>() {

                    //--------------------------------------------------------------
                    //1)Carico Properties 
                    //--------------------------------------------------------------
                     //--------------------------------------------------------------
                    //2)Creo il pool di Connessioni
                    //--------------------------------------------------------------
                    DBManager dbPool = DBManager.INSTANCE;

                    @Override
                    public void call(Iterator<ZBRecord> items) throws Exception {

                        ZBoxDAO dao = new ZBoxDAO();
                        ZBRecordsDAO recordDao = new ZBRecordsDAO();
                        Connection conn = null;
                        //CloseableHttpClient httpConn = null;
                        try{
                            int counter = 0;
                            //------------------------------------------------------
                            //2)Prendo una connessione jdbc
                            //------------------------------------------------------
                            conn = dbPool.getConnection();

                            //httpConn = httpPool.getConnection();
                            //conn

                             while ( items.hasNext() ) {
                                 
                                 
                                ZBRecord record = null; 
                                //try{ 

                                    record = items.next(); 
                                    //--------------------------------------------------
                                    //2.1 salvo l'evento E e recupero idZBEvent (counter
                                    // della tabella
                                    //--------------------------------------------------
                                    long idZBEvent = 0;
                                    //if (record!=null) 
                                        idZBEvent = recordDao.InsertZBRecordE(conn,record); 



                                     //-----------------------------------------------------------------------
                                     //Se l'evento è già presente, idZBEvent ritorna a 0 ; passo al successivo
                                     //-----------------------------------------------------------------------
                                     //da Commentare per TEST
                                     if (idZBEvent == 0) {
                                         logger.debug("Evento già presente per la box:"+record.getIDBlackBox()+". Verra' ignorato.");
                                         continue;
                                     } 


                                    //logger.debug("idZBEvent recuperato:"+idZBEvent);

                                    //------------------------------------------------------
                                    //3) Recupero la Zbox
                                    //------------------------------------------------------
                                    long idBlackBox = record.getIDBlackBox();
                                    ZBox zbox = dao.GetZBoxExtended(conn, new Long(idBlackBox).intValue());

                                    if(zbox!=null){
                                        logger.debug("ZBox recuperata:"+idBlackBox+" recuperata. Inserimento del comand.");
                                        //<-------------------
                                        //<--TODO: Da testare 
                                        //<-------------------
                                        //INSERT su ZCommands se l'evento è di tipo 2
                                        dao.manageSpecialEvents(conn, zbox, record); 

                                        //------------------------------------------------------
                                        // Recupero Voucher e Telefono
                                        //------------------------------------------------------
                                        dao.getExtraInformation(conn, zbox);

                                    }else {
                                        //----------------------------------------------
                                        // GESTIONE se ZBox non trovata : log e proseguo
                                        //----------------------------------------------
                                        logger.debug("ZBox recuperata:"+idBlackBox+" NON TROVATA!!!");
                                        continue;
                                    }


                                    //------------------------------------------------------
                                    // creator costruisce il sender per la zbox individuata
                                    // per DMS, IMA, 
                                    //------------------------------------------------------
                                     FactoryCreator creator = new FactoryCreator(dao, conn );

                                     ArrayList<GenericSender> senders = creator.createSender(record, zbox, idZBEvent);


                                    if (  ( senders != null )&& (senders.size()>0)  ){

                                        for(int s=0; s<senders.size();s++){
                                            //logger.debug("Procedo all'invio per idBlackBox:"+idBlackBox+".");
                                            GenericSender sender = senders.get(s);
                                            sender.send();
                                        }


                                    }
                                    counter++;
                               // }catch(Exception e){
                               //     if (record !=null)
                               //         logger.error("1-Error saving ZbRecords"+record.getIDBlackBox()+" :"+e.toString(),e);
                               //     else logger.error("1-Error saving ZbRecords :"+e.toString(),e);
                               //     logger.error("Stack:"+Utils.getStackTrace(e));
                                //}
                            }//while
                             
                             
                            //dao.commit(conn);

                            //LOG DI TEST
                            //logger.debug("....Successfully executed after "+counter+" ZbRecords");

                        }catch(Exception e){
                             logger.error("2-Error commit ZbRecords :"+e.toString(),e);
                             logger.error("Stack:"+Utils.getStackTrace(e));
                             //dao.rollback(); 

                             //---------------------------------------------
                             //TODO: HANDLE ERROR : salvare sul filesystem??
                             // 1) se il db è giù 
                             //----------------------------------------------
                        }finally{
                             //-------------------------------------------
                             //3)Rilascio la connessione
                             //-------------------------------------------
                             try{
                                if (dao!=null)
                                    dao.close(conn);
                             }catch(SQLException e){
                                 logger.error("Error Closing Connection :"+e.toString(),e);
                             }
                         }
                     }

                 });

             }//fine metodoCall


        });//fine metodoforeachRDD
        
        return jssc;
    }
    
    
    
    
    //-----------------------------------------
    // Recupera informazioni dalla coda Kafka
    //  Topic, Partition, Offset
    //-----------------------------------------
    public static void  displayTestOffsetKafka(JavaPairInputDStream<String, String> messages){
        messages.foreachRDD(
        new Function<JavaPairRDD<String,String>, Void>() {
        @Override
        public Void call(JavaPairRDD< String, String> rdd) throws IOException
        {//OffsetRange[] offsetRanges = ((HasOffsetRanges)rdd).offsetRanges(); // offsetRanges.length = # of Kafka partitions being consumed ... 
            OffsetRange[] offsetRanges = ((HasOffsetRanges) rdd.rdd()).offsetRanges();
            if (offsetRanges != null){
                int dime = offsetRanges.length;
                //logger.info(">>>>>>>>>>>>>>>>>>>>>>>>>>>>OK!!!");
                for(int i=0; i<dime; i++){
                    long from = offsetRanges[i].fromOffset();
                    long until = offsetRanges[i].untilOffset();
                    String topic = offsetRanges[i].topic();
                    int partition = offsetRanges[i].partition();
                    logger.debug("KAFKA - Topic:"+topic+" - Partition:"+partition+" - OFFSET from:"+from+" to:"+until+" ");
                }
            }else{
                //logger.info("KO<<<<<<<<<<<<<<<<<<<<<<<<<<<<");
            }
          
        return null; }
        });

    }
}
