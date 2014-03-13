/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package eu.unitn.disi.db.spleetter.cogroup;

import eu.stratosphere.api.java.record.functions.CoGroupFunction;
import eu.stratosphere.util.Collector;
import eu.stratosphere.api.java.record.functions.FunctionAnnotation;
import eu.stratosphere.types.Record;
import eu.stratosphere.types.IntValue;
import eu.stratosphere.types.LongValue;
import eu.unitn.disi.db.spleetter.TweetCleanse;
import java.io.Serializable;
import java.util.Iterator;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * For all the english words is outputs the tweets containing it.
 * The  tweet id is assumed to be at position <code>1</code>
 * in the record. The other fields are not modified.
 *
 * 0 - tweet id
 * 1 - user id
 * 2 - 1
 */
@FunctionAnnotation.ConstantFieldsFirst({})
@FunctionAnnotation.ConstantFieldsSecond({})
public class EnglishDictionaryCoGroup extends CoGroupFunction  implements Serializable {
    private static final Log LOG = LogFactory.getLog(EnglishDictionaryCoGroup.class);
    private long counter = 0;

    private IntValue one = new IntValue(1);
    private Record outputRecord  = new Record();
    private LongValue tid;
    private LongValue uid;

    @Override
    public void coGroup(Iterator<Record> wordJoin, Iterator<Record> dictJoin, Collector<Record> records) {
        Record pr;
        if (dictJoin.hasNext() && wordJoin.hasNext()) {
            while(wordJoin.hasNext()) {
                pr = wordJoin.next();
                tid = pr.getField(1, LongValue.class);
                uid = pr.getField(2, LongValue.class);
                outputRecord.setField(0, tid);
                outputRecord.setField(1, uid);
                outputRecord.setField(2, one);
                records.collect(outputRecord);
                if(TweetCleanse.EnglishDictionaryCoGroupLog){
                  //System.out.printf("EDCG out\n");
                  this.counter++;
                }
            }
        }
    }

    @Override
    public void close() throws Exception {
        if(TweetCleanse.EnglishDictionaryCoGroupLog){
            LOG.fatal(counter);
        }
    	super.close();
    }
}
