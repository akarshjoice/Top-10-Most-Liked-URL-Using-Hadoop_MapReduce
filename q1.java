import java.io.IOException;
import java.util.StringTokenizer;
import java.util.Map; 
import java.util.TreeMap;
import org.apache.hadoop.io.LongWritable;
import java.util.*;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.conf.Configured;

public class ReduceJoin {

  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, Text>{

   // private final static IntWritable one = new IntWritable(1);
    //private Text word = new Text();
   // private Text url = new Text();
      public static String loc;
 public void map(Object key, Text value, Context context) throws IOException, InterruptedException 
 { 
 String space=" ";
 String str = value.toString();
 String[] parts = str.split(","); 
 Configuration conf = context.getConfiguration();
 String param = conf.get("location");
  
 if(param.equals(parts[3]))
	 context.write(new Text(parts[1]), new Text(parts[4] + " " + parts[2] )); 
 }
  }

  public static class IntSumReducer
       extends Reducer<Text,Text,Text,Text> {
   
    private TreeMap<Long, String> tmap2,tmap3,tmap4,tmap5,tmap6; 
    
    @Override
    public void setup(Context context) throws IOException, 
                                     InterruptedException 
    { 
        tmap2 = new TreeMap<Long, String>(Collections.reverseOrder()); 
        tmap3 = new TreeMap<Long, String>(Collections.reverseOrder());
        tmap4 = new TreeMap<Long, String>(Collections.reverseOrder()); 
        tmap5 = new TreeMap<Long, String>(Collections.reverseOrder()); 
        tmap6 = new TreeMap<Long, String>(Collections.reverseOrder());
    }
 
    @Override 
    public void reduce(Text key, Iterable<Text> values,
                       Context context
                       ) throws IOException, InterruptedException {
      long y=0,t=0,a1=0,a2=0,a3=0;
      String name = key.toString(); 
      
      for (Text val : values) 
      {   String parts[] = val.toString().split(" ");
          //String s=val.toString();
         if( parts[0].equals("male"))
               y =y + 1;
         else 
               t = t + 1;
      
         long age = Integer.parseInt(parts[1]);
         if(age>0 && age<=10)
              a1=a1+1; 
         else if(age>10 && age<20)
              a2=a2+1;
         else if(age>=20)
              a3=a3+1;

      }

   
      if(y!=0)
        {
          
         
	if(tmap2.containsKey(y))
            tmap2.put(y, name + "," + tmap2.get(y));
         else
            tmap2.put(y, name);
        
        }
      if(t!=0)
        {
          
         if(tmap3.containsKey(t))
            tmap3.put(t, name + "," + tmap3.get(t));
         else
            tmap3.put(t, name);
           

        }
      if(a1!=0)
        { if(tmap4.containsKey(a1))
            tmap4.put(a1, name + "," + tmap4.get(a1));
          else
            tmap4.put(a1, name);
        }
      if(a2!=0)
        { if(tmap5.containsKey(a2))
            tmap5.put(a2, name + "," + tmap5.get(a2));
          else
            tmap5.put(a2, name);
        } 
      if(a3!=0)
        { if(tmap6.containsKey(a3))
            tmap6.put(a3, name + "," + tmap6.get(a3));
          else
            tmap6.put(a3, name);
        }
  
        // we remove the first key-value 
        // if it's size increases 10 
        if (tmap2.size() > 10) 
        { 
            tmap2.remove(tmap2.lastKey()); 
        } 

        if (tmap3.size() > 10) 
        { 
            tmap3.remove(tmap3.lastKey()); 
        }
         if (tmap4.size() > 10) 
        { 
            tmap4.remove(tmap4.lastKey()); 
        } 
 	 if (tmap5.size() > 10) 
        { 
            tmap5.remove(tmap5.lastKey()); 
        } 
	 if (tmap6.size() > 10) 
        { 
            tmap6.remove(tmap6.lastKey()); 
        } 
      
      //String str = String.format("%d", y);  
      //context.write(key, new Text(str));
    }

    @Override
    public void cleanup(Context context) throws IOException, 
                                       InterruptedException 
    { 
        int i=0;
        context.write(new Text("\t"), new Text("male"));
        for (Map.Entry<Long, String> entry : tmap2.entrySet())  
        { 
  
            long count = entry.getKey(); 
            String[] name = entry.getValue().split(",");
         for(int j=0;j<name.length;j++)
             { if(i==10)
                    break;
                context.write(new Text("" + count), new Text(name[j])); 
  		i=i+1;
                 
             }
        } 
        i=0;
	context.write(new Text("\n\t"), new Text("female"));
        for (Map.Entry<Long, String> entry : tmap3.entrySet())  
        { 
  
            long count = entry.getKey(); 
            String[] name = entry.getValue().split(",");
           for(int j=0;j<name.length;j++)
             { if(i==10)
                    break;
                context.write(new Text("" + count), new Text(name[j])); 
  		i=i+1;
                 
             }
        } 
        
        i=0;
	context.write(new Text("\n\t"), new Text("below_10yrs"));
        for (Map.Entry<Long, String> entry : tmap4.entrySet())  
        { 
  
            long count = entry.getKey(); 
            String[] name = entry.getValue().split(",");
         for(int j=0;j<name.length;j++)
             { if(i==10)
                    break;
                context.write(new Text("" + count), new Text(name[j])); 
  		i=i+1;
                 
             }

        }
        i=0;
	context.write(new Text("\n\t"), new Text("above_10yrs and below_20yrs"));
        for (Map.Entry<Long, String> entry : tmap5.entrySet())  
        { 
  
            long count = entry.getKey(); 
           // String name = entry.getValue();
            //String c = String.format("%ld", count); 
            String[] name = entry.getValue().split(",");
         for(int j=0;j<name.length;j++)
             { if(i==10)
                    break;
                context.write(new Text("" + count), new Text(name[j])); 
  		i=i+1;
                 
             }

        }
	i=0;
 	context.write(new Text("\n\t"), new Text("above_20yrs"));
        for (Map.Entry<Long, String> entry : tmap6.entrySet())  
        { 
  
            long count = entry.getKey(); 
          //  String name = entry.getValue();
            //String c = String.format("%ld", count); 
	     String[] name = entry.getValue().split(",");
             for(int j=0;j<name.length;j++)
             { if(i==10)
                    break;
                context.write(new Text("" + count), new Text(name[j])); 
  		i=i+1;
                 
             }

        }

 }
}

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
   
   if (otherArgs.length != 3) 
       {
         System.err.println("Usage: ReduceJoin <in> <out> <location> ");
	 System.exit(2);
         
      }
  conf.set("location",args[2]);
    Job job = Job.getInstance(conf, "Reduce-side join");
    job.setJarByClass(ReduceJoin.class);
    job.setMapperClass(TokenizerMapper.class);
   // job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);

    job.setMapOutputKeyClass(Text.class); 
    job.setMapOutputValueClass(Text.class);  
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

 
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}