/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package eu.unitn.disi.db.spleetter.map;

import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.MapStub;
import eu.stratosphere.pact.common.stubs.StubAnnotation;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.common.type.base.PactLong;
import eu.stratosphere.pact.common.type.base.PactString;

/**
 * Filters the tweet record keeping only the user id
 * 0 - tweet id
 * 1 - user id
 * 2 - timestamp [h]
 */
@StubAnnotation.ConstantFields(fields = {0,1})
@StubAnnotation.OutCardBounds(lowerBound = 1, upperBound = 1)
public class UserTweetExtractMap extends MapStub {
    private PactRecord pr2 = new PactRecord(3);

    @Override
    public void map(PactRecord pr, Collector<PactRecord> records) throws Exception {
        pr2.setField(0, pr.getField(0, PactLong.class ));
        pr2.setField(1, pr.getField(1, PactInteger.class ));
        pr2.setField(2, pr.getField(4, PactString.class ));
        records.collect(pr2);
    }
}
