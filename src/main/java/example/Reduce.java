package example;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Reduce extends Reducer<Text, IntWritable, Text, Text>{
   protected void reduce(Text key, Iterable<IntWritable> values, Context ctx){
     int sum = 0;
     for(IntWritable value : values){
       sum += value.get();
     }
     try{
    	 ctx.write(new Text(key), new Text(String.valueOf(sum)));
     } catch(IOException e){
       e.printStackTrace();
     } catch(InterruptedException e){
       e.printStackTrace();
     }
   }
}