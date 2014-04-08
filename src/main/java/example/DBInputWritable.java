package example;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;

public class DBInputWritable implements Writable, DBWritable
{
   private int id;
   private String customer;
   private int amount;

   public void readFields(DataInput in) throws IOException{  
	   //Not implemented
   }

   public void readFields(ResultSet rs) throws SQLException{
     id = rs.getInt(1);
     customer = rs.getString(2);
     amount = rs.getInt(3);
   }

   public void write(DataOutput out) throws IOException{
	 //Not implemented
   }

   public void write(PreparedStatement ps) throws SQLException{
     ps.setInt(1, id);
     ps.setString(2, customer);
   }

   public int getId(){
     return id;
   }

   public String getCustomer(){
     return customer;
   }
   
   public int getAmount(){
	   return amount;
   }

   public void setCustomer(String customer) {
	   this.customer = customer;
   }
	
   public void setAmount(int amount) {
	 this.amount = amount;
   }
   
}