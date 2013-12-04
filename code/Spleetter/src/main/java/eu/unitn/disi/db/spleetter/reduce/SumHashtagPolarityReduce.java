/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package eu.unitn.disi.db.spleetter.reduce;

import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.ReduceStub;
import eu.stratosphere.pact.common.stubs.StubAnnotation;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactDouble;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.common.type.base.PactString;
import eu.unitn.disi.db.spleetter.TweetCleanse;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Iterator;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * For each timestamp, for each hashtag sums the negative, positive polairty and
 * the emotional divergence
 *
 * 0 - timestamp [h]
 * 1 - hashtag
 * 2 - sum neg polarity
 * 3 - sum pos polarity
 * 4 - sum emotional divergence
 */
@StubAnnotation.ConstantFields({0})
public class SumHashtagPolarityReduce extends ReduceStub implements Serializable{
    private static final Log LOG = LogFactory.getLog(SumHashtagPolarityReduce.class);
    private long counter = 0;
    private HashMap<Integer, PactDouble[]> hashtagPolarities = new HashMap<Integer, PactDouble[]>();
    private PactRecord pr2 = new PactRecord(5);
    private PactInteger pactHashtagID = new PactInteger();

    @Override
    public void reduce(Iterator<PactRecord> matches, Collector<PactRecord> records) throws Exception {
        Integer hashtagID;
        PactDouble negPolarity;
        PactDouble posPolarity;
        PactDouble divergence;
        PactDouble[] hashtagValues;


        PactRecord pr = null;
        double temp = 0;
        hashtagPolarities.clear();

        while (matches.hasNext()) {
            pr = matches.next();
            hashtagID = pr.getField(1, PactInteger.class).getValue();

            negPolarity = pr.getField(2, PactDouble.class);
            posPolarity = pr.getField(3, PactDouble.class);
            divergence = new PactDouble();
            divergence.setValue((posPolarity.getValue() - negPolarity.getValue()) / 10);

            if (hashtagPolarities.containsKey(hashtagID)) {
                hashtagValues = hashtagPolarities.get(hashtagID);
                temp = hashtagValues[0].getValue() +  negPolarity.getValue();
                hashtagValues[0].setValue(temp);

                temp = hashtagValues[1].getValue() +  posPolarity.getValue();
                hashtagValues[1].setValue(temp);

                temp = hashtagValues[2].getValue() +  divergence.getValue();
                hashtagValues[2].setValue(temp);

            } else {
                hashtagValues = new PactDouble[3];
                hashtagValues[0]= negPolarity;
                hashtagValues[1]= posPolarity;
                hashtagValues[2]= divergence;
                hashtagPolarities.put(hashtagID, hashtagValues);
            }
        }


        for (Integer hID : hashtagPolarities.keySet()) {
            hashtagValues = hashtagPolarities.get(hID);
            pactHashtagID.setValue(hID);
            pr2.setField(0, pr.getField(0, PactString.class));
            pr2.setField(1, pactHashtagID);
            pr2.setField(2, hashtagValues[0]);
            pr2.setField(3, hashtagValues[1]);
            pr2.setField(4, hashtagValues[2]);
            records.collect(pr2);
            if (TweetCleanse.SumHashtagPolarityReduceLog) {
                //System.out.printf("CEWR out %d \n", pr.getField(0, PactLong.class).getValue() );
                this.counter++;
            }
        }
    }

    @Override
    public void close() throws Exception {
        if (TweetCleanse.SumHashtagPolarityReduceLog) {
            LOG.fatal(counter);
        }
        super.close();
    }
}
