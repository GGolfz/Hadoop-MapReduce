import java.io.IOException;
import java.math.BigDecimal;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

class BalanceDetectorMapper extends Mapper<LongWritable, Text, Text, Text> {

  private Text outputKey = new Text();
  private Text outputValue = new Text();
  private String prev;

  public void map(LongWritable key, Text value, Context context)
    throws IOException, InterruptedException {
    if (
      value.toString().equals("Date,Description,Deposits,Withdrawls,Balance")
    ) {
      return;
    }
    String[] data = value.toString().split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
    for (int i = 0; i < data.length; i++) {
      data[i] = data[i].replaceAll(",", "").replaceAll("\"", "");
    }
    if (prev == null) {
      prev = data[data.length - 1];
    } else {
      BigDecimal currentBalance = new BigDecimal(data[data.length - 1]);
      BigDecimal depositValue = new BigDecimal(data[data.length - 3]);
      BigDecimal withdrawalValue = new BigDecimal(data[data.length - 2]);
      BigDecimal initialBalance = new BigDecimal(prev);
      BigDecimal calculatedBalance = initialBalance
        .add(depositValue)
        .subtract(withdrawalValue);
      if (currentBalance.compareTo(calculatedBalance) != 0) {
        outputKey.set("Date: " + data[0] + " Description: " + data[1]);
        outputValue.set("Initial Balance: " + prev + " Deposit: "+ data[data.length - 3] + " Withdrawal: " +data[data.length - 2] + " Calculated Balance: " + calculatedBalance.toString() + " Recorded  Balance: "+ data[data.length - 1] + " Difference: " + calculatedBalance.subtract(currentBalance));
        context.write(outputKey, outputValue);
      }
      prev = data[data.length - 1];
    }
  }
}

public class BalanceDetector {

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "BalanceDetector");
    job.setJarByClass(BalanceDetector.class);
    job.setMapperClass(BalanceDetectorMapper.class);
    job.setNumReduceTasks(0);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
