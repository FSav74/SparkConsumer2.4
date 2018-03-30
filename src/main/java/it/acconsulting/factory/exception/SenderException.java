/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package it.acconsulting.factory.exception;

/**
 *
 * @author F.Saverio Letterese
 */
public class SenderException extends Exception{

	
	/**
	 * 
	 */
	private static final long serialVersionUID = -7373744754135110586L;

	public SenderException() {
		
		super();
	}

	public SenderException(String message) {
		
		super(message);
	}
	


	public SenderException(Throwable cause) {
		
		super(cause);
	}
	
	public SenderException(String message, Throwable cause) {
		
		super(message, cause);
	}

}



