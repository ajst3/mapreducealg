
import java.io.*;
import java.util.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;

public class Least5_Mapper extends Mapper<Object,
                            Text, Text, LongWritable> {

    private TreeMap<Long, String> tmap;
    private HashMap<String, Long> hmap;

    @Override
    public void setup(Context context) throws IOException,
                                     InterruptedException
    {
        tmap = new TreeMap<Long, String>();
        hmap = new HashMap<String, Long>();
    }

    @Override
    public void map(Object key, Text value,
       Context context) throws IOException,
                      InterruptedException
    {
        HashMap<String, Long> counts = new HashMap<String, Long>();
        StringTokenizer itr = new StringTokenizer(value.toString());
        while(itr.hasMoreTokens())
        {
          String curr = itr.nextToken();
          StringTokenizer counter = new StringTokenizer(value.toString());
          long currcount = 0;
          while(counter.hasMoreTokens())
          {
            if(curr.equals(counter.nextToken()))
              ++currcount;
          }
          counts.put(curr, currcount);
        }

        for(String k : counts.keySet())
        {
          if(hmap.containsKey(k))
          {
            long there = hmap.get(k);
            long toput = there + counts.get(k);
            hmap.put(k, toput);
          }
          else
          {
            hmap.put(k, counts.get(k));
          }
        }

    }

    @Override
    public void cleanup(Context context) throws IOException,
                                       InterruptedException
    {
        TreeMap<Long, String> sorter = new TreeMap<Long, String>();
        for(String k : hmap.keySet())
        {
          sorter.put(hmap.get(k), k);
        }

        for(int i = 0; i < 5; ++i)
        {
          long wordcount = sorter.firstKey();
          String word = sorter.get(wordcount);
          context.write(new Text(word), new LongWritable(wordcount));
          sorter.remove(wordcount);
        }
    }
}
