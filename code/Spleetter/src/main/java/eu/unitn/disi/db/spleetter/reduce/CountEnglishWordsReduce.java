package eu.unitn.disi.db.spleetter.reduce;

import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.ReduceStub;
import eu.stratosphere.pact.common.stubs.StubAnnotation;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.common.type.base.PactLong;
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
@StubAnnotation.ConstantFields(fields = {0})
@StubAnnotation.OutCardBounds(lowerBound = 1, upperBound = 1)
public class CountEnglishWordsReduce extends ReduceStub {

    private static final Log LOG = LogFactory.getLog(CountEnglishWordsReduce.class);
    private long counter = 0;
    PactInteger numWords = new PactInteger();

    @Override
    public void reduce(Iterator<PactRecord> matches, Collector<PactRecord> records) throws Exception {
        PactRecord pr = null;
        int sum = 0;

        while (matches.hasNext()) {
            pr = matches.next();
            sum += pr.getField(1, PactInteger.class).getValue();
        }
        numWords.setValue(sum);
        pr.setField(1, numWords);

        records.collect(pr);
        if (TweetCleanse.CountEnglishWordsReduceLog) {
            //System.out.printf("CEWR out %d \n", pr.getField(0, PactLong.class).getValue() );
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
