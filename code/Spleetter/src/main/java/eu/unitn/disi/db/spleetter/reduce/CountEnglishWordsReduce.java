package eu.unitn.disi.db.spleetter.reduce;

import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.ReduceStub;
import eu.stratosphere.pact.common.stubs.StubAnnotation;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactInteger;
import java.util.Iterator;

/**
 * For each tweets it counts the number of english words
 * present in it
 *
 * 0 - tweet id
 * 1 - number of words
 *
 *
 */
@StubAnnotation.ConstantFields(fields = {0})
@StubAnnotation.OutCardBounds(lowerBound = 1, upperBound = 1)
public class CountEnglishWordsReduce extends ReduceStub {
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
    }
}
