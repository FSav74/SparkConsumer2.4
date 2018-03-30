/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package it.acconsulting.locator;

/**
 *
 * @author F.Saverio Letterese
 */
public class ServiceLocatorException extends Exception {
	/**
	 * 
	 */
	private static final long serialVersionUID = 8094369331102768662L;

	
	
	/**
	 * 
	 */
	public ServiceLocatorException() {
	
		super();
	}

	/**
	 * @param message
	 */
	public ServiceLocatorException(String message) {

		super(message);
	}

	/**
	 * @param cause
	 */
	public ServiceLocatorException(Throwable cause) {
	
		super(cause);
	}

	/**
	 * @param message
	 * @param cause
	 */
	public ServiceLocatorException(String message, Throwable cause) {
	
		super(message, cause);
	}

}