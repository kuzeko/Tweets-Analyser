/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package eu.unitn.disi.db.spleetter.cogroup;

import eu.stratosphere.pact.common.stubs.CoGroupStub;
import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.StubAnnotation;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.common.type.base.PactLong;
import java.util.Iterator;

/**
 * Sums up the counts for a certain given key. The counts are assumed to be at position <code>1</code>
 * in the record. The other fields are not modified.
 */
@StubAnnotation.ConstantFieldsFirst(fields = {})
@StubAnnotation.ConstantFieldsSecond(fields = {})
@StubAnnotation.OutCardBounds(lowerBound = 0, upperBound = 1)
public class EnglishDictionaryCoGroup extends CoGroupStub {        
    private PactInteger one = new PactInteger(1);
    private PactRecord outputRecord  = new PactRecord();
    private PactLong tid;

    @Override
    public void coGroup(Iterator<PactRecord> wordMatch, Iterator<PactRecord> dictMatch, Collector<PactRecord> records) {
        PactRecord pr;
        if (dictMatch.hasNext()) {
            while(wordMatch.hasNext()) {
                pr = wordMatch.next();

                tid = pr.getField(1, PactLong.class);
                outputRecord.setField(0, tid);
                outputRecord.setField(1, one);
                records.collect(outputRecord);
            }
        }             
    }
}
