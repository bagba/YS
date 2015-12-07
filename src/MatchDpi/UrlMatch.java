/**
 * @author Mock
 * 
 *         2015年10月26日 下午6:22:30
 */
package MatchDpi;

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.HashSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import Builder.Mobile_4G_builder;
import Model.Mobile_4G_LogEntity;
import UrlMatcher.DaoRule;
import UrlMatcher.OwnRule;


public class UrlMatch {


  // private static Logger logger = LoggerFactory.getLogger(UrlMatch.class);



  public static void main(String[] args) throws IOException, InterruptedException,
      ClassNotFoundException {

    Configuration conf = new Configuration();
    conf.set("mapreduce.job.queuename", "ven4");
    conf.set("test", "yeah");
    conf.set("mapred.textoutputformat.ignoreseparator", "true");
    conf.set("mapred.textoutputformat.separator", ",");
    // String regexPath ="/user/yuanshu/lzd/cache/regex.txt";
    if (args.length != 6) {
      System.out.println("invalid input");
      return;
    }
    String regexPath = args[args.length - 1];
    String inputPath = args[0];
    String outputPath = args[1];
    String type = args[2];
    String pvuv = args[3];
    String sequence=args[4];
    conf.set("type", type);
    conf.set("pvuv", pvuv);

    @SuppressWarnings("deprecation")
    Job job = new Job(conf, "jobMapReduce");
    job.setJarByClass(UrlMatch.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    job.setMapperClass(CountMapper.class);
    job.setReducerClass(CountReducer.class);

    // job.setNumReduceTasks(1);

    FileSystem fs = FileSystem.get(conf);
    fs.delete(new Path(outputPath), true);

    job.addCacheFile(new Path(regexPath).toUri());
    if(sequence.equals("0")){
      MultipleInputs.addInputPath(job, new Path(inputPath),TextInputFormat.class,
          CountMapper.class);
    }
    else if(sequence.equals("1")){
      MultipleInputs.addInputPath(job, new Path(inputPath), SequenceFileInputFormat.class,
          CountMapper.class);
    }
    else {
      System.out.println("invalid input");
      return;
    }
//    FileInputFormat.setInputPaths(job, inputPath);
    FileOutputFormat.setOutputPath(job, new Path(outputPath));
    FileInputFormat.setInputPathFilter(job, FileFilter.class);

    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }

  static class FileFilter implements PathFilter {
    public boolean accept(Path path) {
      // TODO Auto-generated method stub
      return !path.getName().endsWith(".tmp");
    }
  }

  public static class CountMapper extends Mapper<LongWritable, Text, Text, Text> {
    HashMap<String, String> urlMap = new HashMap<>();
    private String type = null;
    private String pvuv = null;
    private HashSet<String> uniqueUser = new HashSet<>();
    private DaoRule rule;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {

      URI[] cacheFile = context.getCacheFiles();
      type = context.getConfiguration().get("type");
      pvuv = context.getConfiguration().get("pvuv");
      rule = new OwnRule(new Path(cacheFile[0]), context.getConfiguration());
    }

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException,
        InterruptedException {
      String url = "";
      String userNum = "";
//      type = context.getConfiguration().get("type");
//      pvuv = context.getConfiguration().get("pvuv");
      switch (type) {
        case "0":
          Mobile_4G_builder builder = Mobile_4G_builder.getInstance();
          Mobile_4G_LogEntity entity = builder.build(value.toString());
          if(entity==null)return;
          url = entity.getUrl();
          if(url.length()>80)
          url = url.substring(0,80);
          userNum = entity.getMismdn();
        case "1":
          break;
        case "3":
          break;
        case "4":
          break;
      }

      if (pvuv.equals("0")) {
        if (rule.matches(url))
          context.write(new Text(rule.getMatched()), new Text("1"));
      } else if (pvuv.equals("1")) {
        if (!(rule.matches(url)) || (this.uniqueUser.contains(userNum + rule.getMatched())))
          return;
        this.uniqueUser.add(userNum + rule.getMatched());
        context.write(new Text(rule.getMatched()), new Text(userNum));
      } else if (pvuv.equals("2")) {
        if (rule.matches(url)) {
          context.write(new Text(rule.getMatched()), new Text("1"));
          if (this.uniqueUser.contains(userNum + rule.getMatched()))
            return;
          this.uniqueUser.add(userNum + rule.getMatched());
          context.write(new Text(rule.getMatched()), new Text(userNum));
        }
      }
//      context.write(new Text(userNum), new Text("1"));
//      context.write(new Text(url), new Text("1"));
    }
  }
  public static class CountReducer extends Reducer<Text, Text, Text, IntWritable> {

    private HashMap<String, HashSet<String>> maps = new HashMap<>();
    private String pvuv;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
      // TODO Auto-generated method stub
      super.setup(context);
      pvuv = context.getConfiguration().get("pvuv");
    }

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException,
        InterruptedException {
//      pvuv = context.getConfiguration().get("pvuv");
      if(pvuv.equals("0")){
        int sum=1;
        while(values.iterator().hasNext()){
          values.iterator().next();
          sum++;
        }
        context.write(key,new IntWritable(sum));
      }
      else if(pvuv.equals("2")){
        int sum=1;
        HashSet<String> tmp;
        String value;
        String keyStr = key.toString().trim();
        while(values.iterator().hasNext()){
          value=values.iterator().next().toString().trim();
          if(value.equals("1"))
          sum++;
          else {
            if (maps.containsKey(keyStr)) {
              tmp = maps.get(keyStr);
              tmp.add(value);
              maps.put(keyStr.toString(), tmp);
            } else {
              HashSet<String> uniqueUser = new HashSet<>();
              uniqueUser.add(value);
              maps.put(keyStr, uniqueUser);
            }
          }
        }
        context.write(new Text(key.toString()+","+maps.get(keyStr).size()),new IntWritable(sum));
      }
      else if(pvuv.equals("1")){
        HashSet<String> tmp;
        String keyStr = key.toString().trim();
        while (values.iterator().hasNext()) {
          if (maps.containsKey(keyStr)) {
            tmp = maps.get(keyStr);
            tmp.add(values.iterator().next().toString().trim());
            maps.put(keyStr.toString(), tmp);
          } else {
            HashSet<String> uniqueUser = new HashSet<>();
            uniqueUser.add(values.iterator().next().toString());
            maps.put(keyStr, uniqueUser);
          }
        }
        context.write(key, new IntWritable(maps.get(keyStr).size()));
      }
    }
    
    
  }
}
