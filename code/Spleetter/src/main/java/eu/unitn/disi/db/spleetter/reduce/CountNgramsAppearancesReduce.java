package eu.unitn.disi.db.spleetter.reduce;

import eu.stratosphere.api.java.record.functions.FunctionAnnotation;
import eu.stratosphere.api.java.record.functions.ReduceFunction;
import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.types.IntValue;
import eu.stratosphere.types.Record;
import eu.stratosphere.util.Collector;
import eu.unitn.disi.db.spleetter.TweetCleanse;
import java.util.Iterator;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * For each words it counts the number of tweets in which it is present
 *
 * 0 - word<br />
 * 1 - number of appearances<br />
 *
 */
@FunctionAnnotation.ConstantFields({0})
public class CountNgramsAppearancesReduce extends ReduceFunction {
    private static final Log LOG = LogFactory.getLog(CountNgramsAppearancesReduce.class);
    private long counter = 0;
    private int numThreshold = 0;
    private IntValue numAppearances = new IntValue();

    /**
    * Reads the filter literals from the configuration.
    *
    * @see eu.stratosphere.pact.common.stubs.Function#open(eu.stratosphere.nephele.configuration.Configuration)
    */
   @Override
   public void open(Configuration parameters) {
           this.numThreshold = Integer.parseInt(parameters.getString(TweetCleanse.APPEARANCE_TRESHOLD, "1"));
   }

    @Override
    public void reduce(Iterator<Record> matches, Collector<Record> records) throws Exception {
        Record pr = null;
        int sum = 0;

        while (matches.hasNext()) {
            pr = matches.next();
            sum++;
        }
        if(sum >= this.numThreshold) {
            numAppearances.setValue(sum);
            pr.setField(1, numAppearances);
            records.collect(pr);
            if (TweetCleanse.CountNgramsAppearancesReduceLog) {
                this.counter++;
            }
        }
    }

    @Override
    public void close() throws Exception {
        if (TweetCleanse.CountNgramsAppearancesReduceLog) {
            LOG.fatal(counter);
        }
        super.close();
    }
}
