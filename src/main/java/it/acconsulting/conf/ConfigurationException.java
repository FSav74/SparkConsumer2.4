/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package it.acconsulting.conf;

/**
 *
 * @author F.Saverio Letterese
 */
public class ConfigurationException extends Exception {
	
	
	
    /**
     * 
     */
    public ConfigurationException() {

            super();
    }

    /**
     * @param message
     */
    public ConfigurationException(String message) {

            super(message);
    }

    /**
     * @param cause
     */
    public ConfigurationException(Throwable cause) {

            super(cause);
    }

    /**
     * @param message
     * @param cause
     */
    public ConfigurationException(String message, Throwable cause) {

            super(message, cause);
    }

}

