
import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Least5_Reducer extends Reducer<Text,
                     LongWritable, Text, LongWritable> {

    private TreeMap<Long, String> tmap2;

    @Override
    public void setup(Context context) throws IOException,
                                     InterruptedException
    {
        tmap2 = new TreeMap<Long, String>();
    }

    @Override
    public void reduce(Text key, Iterable<LongWritable> values,
      Context context) throws IOException, InterruptedException
    {

        // input data from mapper
        // key                values
        // movie_name         [ count ]
        String name = key.toString();
        long totalcount = 0;
        for (LongWritable val : values)
        {
            totalcount += val.get();
        }

        if (tmap2.size() > 5)
        {
            tmap2.remove(tmap2.lastKey());
        }

    }

    @Override
    public void cleanup(Context context) throws IOException,
                                       InterruptedException
    {

        for (Map.Entry<Long, String> entry : tmap2.entrySet())
        {

            long count = entry.getKey();
            String name = entry.getValue();
            context.write(new Text(name), new LongWritable(count));
        }
    }
}
