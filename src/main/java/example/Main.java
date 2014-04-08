package example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Main {
   public static void main(String[] args) throws Exception {
	   
	 System.out.println("Executing map-reduce ...");  
     Configuration conf = new Configuration();
     if(args.length != 5){ 
    	 System.out.println("Invalid number of arguments -> user arguments as [dbserver,database,user,pwd,output-path]");
    	 System.exit(1);
     }
    		 
     String dbServer = args[0];
     String database = args[1];
     String user = args[2];
     String pwd = args[3];
     
     DBConfiguration.configureDB(conf,
     "com.mysql.jdbc.Driver",   // driver class
     "jdbc:mysql://"+dbServer+":3306/"+database, // db url
     user,    // user name
     pwd); //password

     Job job = new Job(conf);
     job.setJarByClass(Main.class);
     job.setMapperClass(Map.class);
     job.setReducerClass(Reduce.class);
     job.setMapOutputKeyClass(Text.class);
     job.setMapOutputValueClass(IntWritable.class);
     job.setOutputKeyClass(Text.class);
     job.setOutputValueClass(Text.class);
    
     job.setInputFormatClass(DBInputFormat.class);
     job.setOutputFormatClass(TextOutputFormat.class);

     DBInputFormat.setInput(
    		 job,
    		 DBInputWritable.class,
    		 "purchase",   //input table name
    		 null,
    		 null,
    		 new String[] { "id", "customer","amount" }  // table columns
    		 );
     FileOutputFormat.setOutputPath(job, new Path(args[4]));
     System.exit(job.waitForCompletion(true) ? 0 : 1);
   }
}