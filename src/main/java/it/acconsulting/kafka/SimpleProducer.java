/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package it.acconsulting.kafka;

import com.google.common.io.Resources; 
import com.google.gson.Gson;
import it.acconsulting.bean.ZBRecord;



import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.log4j.Logger;

/**
 * Il Producer è gestito tramite Singleton.
 * Per performance è consigliabile riutilizzare lo stesso Producer che ottimizza
 * l'utilizzo del buffer di accodamento.
 * 
 * TO-BE: Utilizzo di un pool di Producer
 * 
 * 
 * @author F.Saverio Letterese
 */
public enum SimpleProducer implements Serializable{
     INSTANCE;

     private Properties properties;
     private transient KafkaProducer<String, String> producer; //Il producer di Kafka non è serializabile 
     
     private Logger logger =  Logger.getLogger("GATEWAY");        
     
     private SimpleProducer(){

        //TODO:-----------------------------------------------------------------
        //TODO: 
        //TODO: Il recupero delle properties è effettuato
        //nel bat di lancio: con l' aggiunta nel classpath la cartella properties dove è
        //presente producer.props
        //TODO:-----------------------------------------------------------------
        try (InputStream props = Resources.getResource("producer.props").openStream()) {
            logger.debug(".................SimpleProducer constructor");
            properties = new Properties();
            properties.load(props);
            producer = new KafkaProducer<>(properties);
        }catch(IOException e){
            	e.printStackTrace();
                //TODO: rivedere gestione errori: qui viene lanciata una runTimeException
                throw new RuntimeException("Errore caricamento file Properties.",e);
        }
   
         
     }
     
     /**
      * Metodo di test coda kafka. Invia solo una stringa alla coda.
      * 
      * 
      * @param idZbox
      * @throws ProducerException 
      */
     /*public void sendSynchronous(String idZbox) throws ProducerException{
       
        System.out.println("SimpleProducer sends synchronous messagge to Kafka.");
        try {
          
            RecordMetadata recordMetaData = producer.send(new ProducerRecord<String, String>(
                        "my-topic",
                        String.format("{\"type\":\"test\", \"t\":%.3f, \"ID\":%s}", System.nanoTime() * 1e-9, idZbox))
            ).get();
            //il .get() rende asincrono il meto: si aspetta la replica del server.
            
            //producer.flush();     
            //----------------------------------------------------------------
            //il metodo get è quello che mi fa passare alla chiamata synchrona 
            //synchronous 
            // e posso recuperare l'esito dell'accodamento
            //
            //---------------------------------------------------------------
            System.out.println("Message produced, offset: " + recordMetaData.offset());
            System.out.println("Message produced, partition: " + recordMetaData.partition());
            System.out.println("Message produced, topic: " + recordMetaData.topic());
            System.out.println("Sent id Zbox " + idZbox);
  
            
        } catch (Throwable throwable) {
            System.out.printf("%s", throwable.getStackTrace());
            throw new ProducerException(throwable);
        }
        //finally {
        //    producer.close();
        //}

    }
    */
    
    /**
     * Invio verso la coda Kafka. 
     * Invio è sincrono : aspetta l'ack da kafka di accodamento.
     * 
     * @param record
     * @throws ProducerException 
     */ 
    public void sendSynchronous(ZBRecord record) throws ProducerException{
       
        System.out.println("SimpleProducer sends synchronous messagge to Kafka.");
        try {
            Gson gson = new Gson();
            String jsonInString = gson.toJson(record);
            
            RecordMetadata recordMetaData = producer.send(new ProducerRecord<String, String>(
                        "my-topic2",
                        jsonInString)
            ).get();
            /*
            producer.send(new ProducerRecord<String, String>(
                        "my-topic",
                        String.format("{\"type\":\"test\", \"t\":%.3f, \"IDBlackBox\":%s \"type\":%s}", System.nanoTime() * 1e-9, record.IDBlackBox, record.getRecordType()))
            ).get();
            */
            //il .get() rende asincrono il meto: si aspetta la replica del server.
            
            //producer.flush();   
            //----------------------------------------------------------------
            //il metodo get è quello che mi fa passare alla chiamata synchrona 
            //synchronous 
            // e posso recuperare l'esito dell'accodamento
            //
            //---------------------------------------------------------------
            System.out.println("Message produced, offset: " + recordMetaData.offset());
            System.out.println("Message produced, partition: " + recordMetaData.partition());
            System.out.println("Message produced, topic: " + recordMetaData.topic());
            System.out.println("Sent id Zbox " + record.IDBlackBox +" record type = "+record.getRecordType());
  
            
        } catch (Throwable throwable) {
            System.out.printf("%s", throwable.getStackTrace());
            throw new ProducerException(throwable);
        }
        //finally {
        //    producer.close();
        //}

    }
     
    
 
     
     
     
    public void closeProducer(){
        System.out.println("SimpleProducer close.");
        producer.close();
    } 
    
    
     
}
