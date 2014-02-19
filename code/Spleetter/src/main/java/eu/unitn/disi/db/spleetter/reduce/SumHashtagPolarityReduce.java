/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package eu.unitn.disi.db.spleetter.reduce;

import eu.stratosphere.api.java.record.functions.FunctionAnnotation;
import eu.stratosphere.api.java.record.functions.ReduceFunction;
import eu.stratosphere.types.DoubleValue;
import eu.stratosphere.types.IntValue;
import eu.stratosphere.types.Record;
import eu.stratosphere.types.StringValue;
import eu.stratosphere.util.Collector;
import eu.unitn.disi.db.spleetter.TweetCleanse;
import java.util.HashMap;
import java.util.Iterator;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * For each timestamp, for each hashtag sums the negative, positive polarity and
 * the emotional divergence
 *
 * 0 - timestamp [h]
 * 1 - hashtag
 * 2 - sum neg polarity
 * 3 - sum pos polarity
 * 4 - sum emotional divergence
 */
@FunctionAnnotation.ConstantFields({0})
public class SumHashtagPolarityReduce extends ReduceFunction{
    private static final Log LOG = LogFactory.getLog(SumHashtagPolarityReduce.class);
    private long counter = 0;
    private HashMap<Integer, DoubleValue[]> hashtagPolarities = new HashMap<Integer, DoubleValue[]>();
    private Record pr2 = new Record(5);
    private IntValue pactHashtagID = new IntValue();

    @Override
    public void reduce(Iterator<Record> matches, Collector<Record> records) throws Exception {
        Integer hashtagID;
        DoubleValue negPolarity;
        DoubleValue posPolarity;
        DoubleValue divergence;
        DoubleValue[] hashtagValues;


        Record pr = null;
        double temp = 0;
        hashtagPolarities.clear();

        while (matches.hasNext()) {
            pr = matches.next();
            hashtagID = pr.getField(1, IntValue.class).getValue();

            negPolarity = pr.getField(2, DoubleValue.class);
            posPolarity = pr.getField(3, DoubleValue.class);
            divergence = new DoubleValue();
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
                hashtagValues = new DoubleValue[3];
                hashtagValues[0]= negPolarity;
                hashtagValues[1]= posPolarity;
                hashtagValues[2]= divergence;
                hashtagPolarities.put(hashtagID, hashtagValues);
            }
        }


        for (Integer hID : hashtagPolarities.keySet()) {
            hashtagValues = hashtagPolarities.get(hID);
            pactHashtagID.setValue(hID);
            pr2.setField(0, pr.getField(0, StringValue.class));
            pr2.setField(1, pactHashtagID);
            pr2.setField(2, hashtagValues[0]);
            pr2.setField(3, hashtagValues[1]);
            pr2.setField(4, hashtagValues[2]);
            records.collect(pr2);
            if (TweetCleanse.SumHashtagPolarityReduceLog) {
                //System.out.printf("CEWR out %d \n", pr.getField(0, LongValue.class).getValue() );
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
