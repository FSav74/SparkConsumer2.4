/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package it.acconsulting.dao;


import java.sql.Connection;
import it.acconsulting.db.DBManager;
import java.sql.SQLException;




/**
 *
 * @author F.Saverio Letterese
 */
public class SimpleDAO {
    
    
        public static Connection getConnection() throws SQLException {
	
                Connection conOut = DBManager.INSTANCE.getConnection();
                //conOut.setAutoCommit(false);
		return conOut;
	}
	

}
