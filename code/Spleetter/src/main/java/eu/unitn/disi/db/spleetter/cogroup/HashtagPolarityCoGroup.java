/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package eu.unitn.disi.db.spleetter.cogroup;

import eu.stratosphere.api.java.record.functions.CoGroupFunction;
import eu.stratosphere.util.Collector;
import eu.stratosphere.api.java.record.functions.FunctionAnnotation;
import eu.stratosphere.types.Record;
import eu.stratosphere.types.DoubleValue;
import eu.stratosphere.types.IntValue;
import eu.unitn.disi.db.spleetter.TweetCleanse;
import java.io.Serializable;
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
 * 5 - tot neg polarity
 * 6 - tot pos polarity
 * 7 - tot divergence
 * 8 - count
 */
@FunctionAnnotation.ConstantFieldsFirst({0,1})
@FunctionAnnotation.ConstantFieldsSecond({})
public class HashtagPolarityCoGroup extends CoGroupFunction {
    private HashMap<Integer, Integer> hashtagTweetsCount = new HashMap<Integer, Integer>();
    private static final Log LOG = LogFactory.getLog(HashtagPolarityCoGroup.class);
    private long counter = 0;

    @Override
    public void coGroup(Iterator<Record> hashtagTweetsSum, Iterator<Record> hashtagPolarities, Collector<Record> records) {
        Record pr;
        Integer hashtagID;
        Integer count;
        Double tmpD;
        DoubleValue negPolarity;
        DoubleValue posPolarity;
        DoubleValue divergence;

        hashtagTweetsCount.clear();

        if (hashtagPolarities.hasNext() && hashtagTweetsSum.hasNext()) {
            while(hashtagTweetsSum.hasNext()){
                pr = hashtagTweetsSum.next();

                hashtagID = pr.getField(1, IntValue.class).getValue();
                count = pr.getField(2, IntValue.class).getValue();

                hashtagTweetsCount.put(hashtagID,count);
            }

            while (hashtagPolarities.hasNext()) {
                pr = hashtagPolarities.next();
                hashtagID = pr.getField(1, IntValue.class).getValue();
                count = hashtagTweetsCount.get(hashtagID);

                // -------
                negPolarity = pr.getField(2, DoubleValue.class);
                pr.setField(5, negPolarity);

                posPolarity = pr.getField(3, DoubleValue.class);
                pr.setField(6, posPolarity);

                divergence = pr.getField(4, DoubleValue.class);
                pr.setField(7, divergence);

                pr.setField(8, new IntValue(count));
                // -------


                tmpD = negPolarity.getValue() / count;
                negPolarity.setValue(tmpD);
                pr.setField(2, negPolarity);

                tmpD = posPolarity.getValue() / count;
                posPolarity.setValue(tmpD);
                pr.setField(3, posPolarity);

                divergence = pr.getField(4, DoubleValue.class);
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
