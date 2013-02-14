/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package eu.unitn.disi.db.spleetter.cogroup;

import eu.stratosphere.pact.common.stubs.CoGroupStub;
import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.StubAnnotation;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactDouble;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.unitn.disi.db.spleetter.TweetCleanse;
import java.util.HashMap;
import java.util.Iterator;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * For each timestamp and each hashtag it computes the mean of the polarity values
 * 0 - timestamp [h]
 * 1 - hashtag
 * 2 - mean neg polarity
 * 3 - mean pos polarity
 * 4 - mean divergence
 */
@StubAnnotation.ConstantFieldsFirst(fields = {0,1})
@StubAnnotation.ConstantFieldsSecond(fields = {})
@StubAnnotation.OutCardBounds(lowerBound = 0, upperBound = 1)
public class HashtagPolarityCoGroup extends CoGroupStub {
    private HashMap<Integer, Integer> hashtagTweetsCount = new HashMap<Integer, Integer>();
    private static final Log LOG = LogFactory.getLog(HashtagPolarityCoGroup.class);
    private long counter = 0;

    @Override
    public void coGroup(Iterator<PactRecord> hashtagTweetsSum, Iterator<PactRecord> hashtagPolarities, Collector<PactRecord> records) {
        PactRecord pr;
        Integer hashtagID;
        Integer count;
        Double tmpD;
        PactDouble negPolarity;
        PactDouble posPolarity;
        PactDouble divergence;

        hashtagTweetsCount.clear();

        if (hashtagPolarities.hasNext() && hashtagTweetsSum.hasNext()) {
            while(hashtagTweetsSum.hasNext()){
                pr = hashtagTweetsSum.next();

                hashtagID = pr.getField(1, PactInteger.class).getValue();
                count = pr.getField(2, PactInteger.class).getValue();

                hashtagTweetsCount.put(hashtagID,count);
            }

            while (hashtagPolarities.hasNext()) {
                pr = hashtagPolarities.next();
                hashtagID = pr.getField(1, PactInteger.class).getValue();
                count = hashtagTweetsCount.get(hashtagID);

                negPolarity = pr.getField(2, PactDouble.class);
                tmpD = negPolarity.getValue() / count;
                negPolarity.setValue(tmpD);
                pr.setField(2, negPolarity);


                posPolarity = pr.getField(3, PactDouble.class);
                tmpD = posPolarity.getValue() / count;
                posPolarity.setValue(tmpD);
                pr.setField(3, posPolarity);


                divergence = pr.getField(4, PactDouble.class);
                tmpD = divergence.getValue() / count;
                divergence.setValue(tmpD);
                pr.setField(4, divergence);

                records.collect(pr);
                if(TweetCleanse.HashtagPolarityCoGroupLog){
                  this.counter++;
                }

            }
        }
    }

    @Override
    public void close() throws Exception {
        if(TweetCleanse.HashtagPolarityCoGroupLog){
            LOG.fatal(counter);
        }
    	super.close();
    }

}
