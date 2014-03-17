package eu.unitn.disi.db.spleetter.join;

import eu.stratosphere.api.java.record.functions.FunctionAnnotation;
import eu.stratosphere.api.java.record.functions.JoinFunction;
import eu.stratosphere.types.IntValue;
import eu.stratosphere.types.LongValue;
import eu.stratosphere.types.Record;
import eu.stratosphere.types.StringValue;
import eu.stratosphere.util.Collector;
import eu.unitn.disi.db.spleetter.TweetCleanse;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Joins the tweet record with its corresponding date appending it to the end of
 * the record
 * 0 - timestamp [h]<br />
 * 1 - ngram
 */
public class CountedNgramsJoin extends JoinFunction {
    private static final Log LOG = LogFactory.getLog(CountedNgramsJoin.class);
    private long counter = 0;
    private Record output = new Record(2);


    @Override
    public void join(Record wordCount, Record wordTime, Collector<Record> records) throws Exception {
        output.setField(0, wordTime.getField(3, StringValue.class));
        output.setField(1, wordTime.getField(0, StringValue.class));

        records.collect(output);
        if (TweetCleanse.CountedNgramsJoin) {
            //System.out.printf("TDM out\n");
            this.counter++;
        }
    }

    @Override
    public void close() throws Exception {
        if (TweetCleanse.CountedNgramsJoin) {
            LOG.fatal(counter);
        }
        super.close();
    }


}
