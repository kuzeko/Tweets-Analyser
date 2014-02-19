package eu.unitn.disi.db.spleetter.reduce;

import eu.stratosphere.api.java.record.functions.FunctionAnnotation;
import eu.stratosphere.api.java.record.functions.ReduceFunction;
import eu.stratosphere.types.IntValue;
import eu.stratosphere.types.Record;
import eu.stratosphere.util.Collector;
import eu.unitn.disi.db.spleetter.TweetCleanse;
import java.util.Iterator;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * For each tweets it counts the number of english words present in it
 *
 * 0 - tweet id<br />
 * 1 - number of words<br />
 *
 *
 */
@FunctionAnnotation.ConstantFields({0})
public class CountEnglishWordsReduce extends ReduceFunction{
    private static final Log LOG = LogFactory.getLog(CountEnglishWordsReduce.class);
    private long counter = 0;
    IntValue numWords = new IntValue();

    @Override
    public void reduce(Iterator<Record> matches, Collector<Record> records) throws Exception {
        Record pr = null;
        int sum = 0;

        while (matches.hasNext()) {
            pr = matches.next();
            sum += pr.getField(1, IntValue.class).getValue();
        }
        numWords.setValue(sum);
        pr.setField(1, numWords);

        records.collect(pr);
        if (TweetCleanse.CountEnglishWordsReduceLog) {
            //System.out.printf("CEWR out %d \n", pr.getField(0, LongValue.class).getValue() );
            this.counter++;
        }
    }

    @Override
    public void close() throws Exception {
        if (TweetCleanse.CountEnglishWordsReduceLog) {
            LOG.fatal(counter);
        }
        super.close();
    }
}
