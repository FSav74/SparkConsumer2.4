/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package it.acconsulting.sparkconsumer;

import com.google.gson.Gson;
import it.acconsulting.bean.ZBRecord;
import it.acconsulting.conf.ConfigurationException;
import it.acconsulting.conf.ConfigurationProperty;

import it.acconsulting.dao.ZBRecordsDAO;
import it.acconsulting.db.DBManager;
import it.acconsulting.util.Utils;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.logging.Level;

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
 *  @author F.Saverio Letterese
 * 
 * -- Consumer per il salvataggio Messaggi DATART (esclusi RECORD E) --
 * 
 * 1)riceve RDD di json e li converte in RDD di oggetti ZBRecord (quelli usati dal udpServer)
 * 2)Salva sul db i messaggi DATART (esclusi RecordType E : eventi che sono gestiti da Kafka2SparkRecordEConsumer)
 * 3)Gestisce checkpoint di Spark.
 * 4)Stampa offset di Spark e il LAG di scodamento 
 * 
 * 
 * Comando di lancio Ambiente di sviluppo:
 * ./spark-submit --class "it.acconsulting.sparkconsumer.Kafka2SparkDBConsumer"  --master spark://192.168.1.107:7077 --deploy-mode cluster --jars /var/opt/spark/SparkConsumer-2.4-jar-with-dependencies.jar --supervise    /var/opt/spark/SparkConsumer-2.4-jar-with-dependencies.jar  192.168.1.75:9092,192.168.1.139:9092,192.168.1.219:9092 ac-topic
 * 
 * 
 */
public class Kafka2SparkDBConsumer {
    private static Logger logger =  Logger.getLogger("GATEWAY");       
   
    public static void main(String[] args) {
        
        final String versione = "19";
        java.util.Date date= new java.util.Date();
	Timestamp timestamp = new Timestamp(date.getTime());
        
        logger.info("["+timestamp+"]Starting Kafka2SparkDBConsumer....."+versione);

        //-------------------------------------
        // Argomenti da linea di comando
        //-------------------------------------
        if (args.length < 2) {
          System.err.println("Usage: Kafka2SparkDBConsumer <brokers> <topics>\n" +
              "  <brokers> is a list of one or more Kafka brokers\n" +
              "  <topics> is a list of one or more kafka topics to consume from\n\n");
          System.exit(1);
        }

        String brokers = args[0];        //lista Broker Kafka
        String topics = args[1];         //lista code Kafka
        
        final String maxCoreApplication; // numero di core Spark per l'applicazione
        final String checkpointFolder;   // folder Spark CheckPoint
        try {
            maxCoreApplication = ConfigurationProperty.ISTANCE.getProperty(ConfigurationProperty.MAX_CORE_APPLICATION);
        } catch (ConfigurationException ex) {
            logger.error("errore configurazione MAX_CORE_APPLICATION.");
            throw new  RuntimeException("Errore configurazione MAX_CORE_APPLICATION. "+ex.toString());
        }
        try {
            checkpointFolder = ConfigurationProperty.ISTANCE.getProperty(ConfigurationProperty.CHECKPOINT_PATH);
        } catch (ConfigurationException ex) {
            logger.error("Errore configurazione CHECKPOINT_PATH. ");
            throw new  RuntimeException("Errore configurazione CHECKPOINT_PATH. "+ex.toString());
        }

        final HashSet<String> topicsSet = new HashSet(Arrays.asList(topics.split(",")));
        final HashMap<String, String> kafkaParams = new HashMap();
        kafkaParams.put("metadata.broker.list", brokers);
        kafkaParams.put("group.id", "acgroup");
        kafkaParams.put("offsets.storage", "kafka");
        kafkaParams.put("dual.commit.enabled", "false");
        
        
        Function0<JavaStreamingContext> createContextFunc = new Function0<JavaStreamingContext>() {
        @Override
        public JavaStreamingContext call() {
            return createContext(checkpointFolder, versione, maxCoreApplication, kafkaParams, topicsSet);
        }
        };

        JavaStreamingContext jssc2 = JavaStreamingContext.getOrCreate(checkpointFolder, createContextFunc);
        jssc2.start();
        jssc2.awaitTermination();

    }//main
     
   
    private static JavaStreamingContext createContext(String checkpointDirectory,String versione, String maxCoreApplication, HashMap<String, String> kafkaParams, HashSet<String> topicsSet){
        
        //-------------------------------------------------
        // Creazione Spark context con intervallo di 2 sec
        //-------------------------------------------------
        SparkConf sparkConf = new SparkConf().setAppName("Kafka2SparkDBConsumer"+versione).set("spark.cores.max", maxCoreApplication);//.set("spark.streaming.kafka.maxRatePerPartition", "1000");//set("spark.executor.cores", "2");        
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
        
         //---------------------------------------------------------
        // Informazioni dalla coda Kafka: Stampa PARTION OFFSET LAG
        //----------------------------------------------------------
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
        //---------------------------------------------------------------
        //LOG di TEST: visualizzo il DStream di Stringhe (in formato jSon)
        //---------------------------------------------------------------
        //lines.print();

        //--------------------------------------------------------------------------
        // Devo filtrare gli Record Type e prendere tutti tranne quelli di tipo E 
        //  (che vengono elaborati da Kafka2SparkrecordEConsumer)
        // Filtro inoltre eventuali record sporchi (se null)
        //--------------------------------------------------------------------------
        JavaDStream<ZBRecord> filteredline = lines2.filter(new Function<ZBRecord,Boolean>() {
            @Override
            public Boolean call(ZBRecord zbrecord) throws Exception {
                if (zbrecord==null) return false;
                //if (zbrecord.getRecordType()==null) return false;
                if (!zbrecord.getRecordType().equals(ZBRecord.RecordTypes.RecordE))
                    return true;
                else return false;
            }

        });
        //---------------------------------------------------------------
        //LOG di TEST: visualizzo il DStream di ZBRecord
        //---------------------------------------------------------------
        //filteredline.print();


        //---------------------------------------------------------------
        // Elaborazione del flusso di ZBRecords
        //---------------------------------------------------------------
        filteredline.foreachRDD(new VoidFunction<JavaRDD<ZBRecord>>() {

             @Override
            public void call(JavaRDD<ZBRecord> item) throws Exception {

                item.foreachPartition(new VoidFunction<Iterator<ZBRecord>>() {

                    //--------------------------------------------------------------
                    //0)Carico Properties 
                    //  sono caricate nella classe DBManager
                    //--------------------------------------------------------------

                    //--------------------------------------------------------------
                    //1)Creo il pool di Connessioni jdbc
                    //--------------------------------------------------------------
                    DBManager pool = DBManager.INSTANCE;

                    @Override
                    public void call(Iterator<ZBRecord> items) throws Exception {

                        ZBRecordsDAO dao = null;
                        Connection conn = null;
                        
                        try{
                            if ((items!=null)&&(items.hasNext())){
                                //------------------------------------------------------
                                //2)Prendo una connessione
                                //------------------------------------------------------
                                conn = pool.getConnection();

                                dao = new ZBRecordsDAO();
                                int counter = 0;
                                Hashtable update = new Hashtable();
                                while ( items.hasNext() ) {

                                    ZBRecord record = items.next();

                                    if (record!=null){
                                        long idLocalization = dao.InsertZBRecord(conn,record,0); 
                                        if (idLocalization!=0)
                                        update.put(record.IDBlackBox,idLocalization);
                                    }

                                    counter++;
                                }

                                dao.commit(conn);

                                if(update!=null){
                                    logger.debug("Size "+update.size() +" da update....");

                                    Enumeration<Long> key = update.keys();
                                    if (key!=null)
                                        while (key.hasMoreElements()){
                                            Long k = key.nextElement();
                                            Long value = (Long)update.get(k);
                                            dao.executeQueryWithRetry(conn,value,k,0);
                                        }
                                    dao.commit(conn);
                                }
                            

                            }
                             
                         }catch(Exception e){
                             logger.error(">A>Error saving ZbRecords :"+e.toString(),e);
                             logger.error("Stack:"+Utils.getStackTrace(e));
                             //dao.rollback(); 

                             //---------------------------------------------
                             //TODO: HANDLE ERROR 
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
    

    
    //-----------------------------------------------
    //  Recupera informazioni dalla coda Kafka
    //  Topic, Partition, Offset
    //-----------------------------------------------
    public static void  displayTestOffsetKafka(JavaPairInputDStream<String, String> messages){
       //---------------------------------------------
        //Stampa l'offset che riceve da Kafka.
        //---------------------------------------------
        messages.foreachRDD(
        new Function<JavaPairRDD<String,String>, Void>() {
        @Override
        public Void call(JavaPairRDD< String, String> rdd) throws IOException
        {//OffsetRange[] offsetRanges = ((HasOffsetRanges)rdd).offsetRanges(); // offsetRanges.length = # of Kafka partitions being consumed ... 
            OffsetRange[] offsetRanges = ((HasOffsetRanges) rdd.rdd()).offsetRanges();
            if (offsetRanges != null){
                int dime = offsetRanges.length;
                logger.debug("-----OFFSET KAFKA----------");
                for(int i=0; i<dime; i++){
                    long from = offsetRanges[i].fromOffset();
                    long until = offsetRanges[i].untilOffset();
                    String topic = offsetRanges[i].topic();
                    int partition = offsetRanges[i].partition();
                    logger.debug("TOPIC:"+topic+" - PARTITION:"+partition+" - OFFSET FROM:"+from+" - TO:"+until+" - LAG:"+(until-from));
                }
            }else{
                logger.debug("KO - OFFSET");
            }
            
        return null; }
        });

    }//displayTestOffsetKafka
    
}//Class
