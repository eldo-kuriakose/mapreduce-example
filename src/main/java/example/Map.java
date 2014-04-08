package example;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Map extends Mapper<LongWritable, DBInputWritable, Text, IntWritable>{

   protected void map(LongWritable id, DBInputWritable value, Context ctx) {
     try{
    	 String customer = value.getCustomer();
    	 ctx.write(new Text(customer),new IntWritable(value.getAmount()));
     } catch(IOException e){
        e.printStackTrace();
     } catch(InterruptedException e){
        e.printStackTrace();
     }
   }
}