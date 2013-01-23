/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package eu.unitn.disi.db.spleetter.reduce;

import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.ReduceStub;
import eu.stratosphere.pact.common.stubs.StubAnnotation;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactInteger;
import java.util.Iterator;

/**
 * 
 */

@StubAnnotation.ConstantFields(fields = {0})
@StubAnnotation.OutCardBounds(lowerBound = 1, upperBound = 1)
public class CountUserTweetsReduce extends ReduceStub {
    
    private PactInteger numTweets = new PactInteger();
    private PactRecord pr2 = new PactRecord(2);
    
    @Override
    public void reduce(Iterator<PactRecord> matches, Collector<PactRecord> records) throws Exception {
        PactRecord pr = null;
        int sum = 0;        
        
        while (matches.hasNext()) {
            pr = matches.next();
            sum ++;
        }
        
        numTweets.setValue(sum);
        //System.out.printf("Tweet: %d, english words: %d\n", pr.getField(0, PactLong.class).getValue(), sum);
        pr2.setField(0, pr.getField(0, PactInteger.class));
        pr2.setField(1, numTweets);
        records.collect(pr2);
    }
}    
