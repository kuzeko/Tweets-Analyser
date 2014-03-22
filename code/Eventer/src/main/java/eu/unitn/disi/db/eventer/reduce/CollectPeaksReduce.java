package eu.unitn.disi.db.eventer.reduce;

import eu.stratosphere.api.java.record.functions.FunctionAnnotation;
import eu.stratosphere.api.java.record.functions.ReduceFunction;
import eu.stratosphere.types.IntValue;
import eu.stratosphere.types.Record;
import eu.stratosphere.types.StringValue;
import eu.stratosphere.util.Collector;
import eu.unitn.disi.db.eventer.TweetPeaks;
import java.util.Iterator;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * For each words it counts the number of tweets in which it is present
 *
 * 0 - day<br />
 * 1 - ngram<br />
 * 2 - num eaks<br />
 *
 */
@FunctionAnnotation.ConstantFields({})
public class CollectPeaksReduce extends ReduceFunction {
    private static final Log LOG = LogFactory.getLog(CollectPeaksReduce.class);
    private long counter = 0;
    private Record prOut = new Record(3);
    private IntValue totalCount = new IntValue(0);


    @Override
    public void reduce(Iterator<Record> matches, Collector<Record> records) throws Exception {
        Record pr = null;
        int sum = 0;

        while (matches.hasNext()) {
            pr = matches.next();
            sum++;
        }

        totalCount.setValue(sum);
        prOut.setField(0, pr.getField(4, StringValue.class));
        prOut.setField(1, pr.getField(0, StringValue.class));
        prOut.setField(2, totalCount);
        records.collect(prOut);
        if (TweetPeaks.CollectContemporaryPeaksLog) {
            this.counter++;
        }

    }

    @Override
    public void close() throws Exception {
        if (TweetPeaks.CollectContemporaryPeaksLog) {
            LOG.fatal(counter);
        }
        super.close();
    }
}
