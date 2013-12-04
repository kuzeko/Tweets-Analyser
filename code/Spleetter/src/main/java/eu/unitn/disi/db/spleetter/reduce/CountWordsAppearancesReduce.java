package eu.unitn.disi.db.spleetter.reduce;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.ReduceStub;
import eu.stratosphere.pact.common.stubs.StubAnnotation;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactInteger;
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
@StubAnnotation.ConstantFields({0})
public class CountWordsAppearancesReduce extends ReduceStub {
    private static final Log LOG = LogFactory.getLog(CountWordsAppearancesReduce.class);
    private long counter = 0;
    private int numThreshold = 0;
    PactInteger numAppearances = new PactInteger();

    /**
    * Reads the filter literals from the configuration.
    *
    * @see eu.stratosphere.pact.common.stubs.Stub#open(eu.stratosphere.nephele.configuration.Configuration)
    */
   @Override
   public void open(Configuration parameters) {
           this.numThreshold = Integer.parseInt(parameters.getString(TweetCleanse.APPEARANCE_TRESHOLD, "1"));
   }

    @Override
    public void reduce(Iterator<PactRecord> matches, Collector<PactRecord> records) throws Exception {
        PactRecord pr = null;
        int sum = 0;

        while (matches.hasNext()) {
            pr = matches.next();
            sum++;
        }
        if(sum >= this.numThreshold) {
            numAppearances.setValue(sum);
            pr.setField(1, numAppearances);
            records.collect(pr);
            if (TweetCleanse.CountWordsAppearancesReduceLog) {
                this.counter++;
            }
        }
    }

    @Override
    public void close() throws Exception {
        if (TweetCleanse.CountWordsAppearancesReduceLog) {
            LOG.fatal(counter);
        }
        super.close();
    }
}
