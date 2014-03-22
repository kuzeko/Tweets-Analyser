package eu.unitn.disi.db.eventer.reduce;

import eu.stratosphere.api.java.record.functions.FunctionAnnotation;
import eu.stratosphere.api.java.record.functions.ReduceFunction;
import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.types.IntValue;
import eu.stratosphere.types.Record;
import eu.stratosphere.types.StringValue;
import eu.stratosphere.util.Collector;
import eu.unitn.disi.db.eventer.TweetPeaks;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;
import java.util.Locale;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * For each hashtag counts the total number of tweets
 *
 * 0 - timestamp[h]
 * 1 - word
 * 2 - count
 * 3 - fake?
 */
@FunctionAnnotation.ConstantFields({})
public class ProduceTimeSeriesReduce extends ReduceFunction {

    private static final Log LOG = LogFactory.getLog(ProduceTimeSeriesReduce.class);
    private long fakeCounter = 0;
    private long trueCounter = 0;
    private SimpleDateFormat dateParser = new SimpleDateFormat("yyyy-MM-dd-hh", Locale.ENGLISH);
    private Date startDateHour = new Date();
    private Date endDateHour = new Date();
    private Date tempDateHour = new Date();
    private Date nextDateHour = new Date();
    private Record prOut = new Record(4);
    private StringValue tempDateValue = new StringValue();
    private IntValue tempCount = new IntValue(0);
    private IntValue notFake = new IntValue(0);
    private IntValue isFake = new IntValue(1);

    /**
     * Reads the filter literals from the configuration.
     *
     * @see
     * eu.stratosphere.pact.common.stubs.Function#open(eu.stratosphere.nephele.configuration.Configuration)
     */
    @Override
    public void open(Configuration parameters) {

        try {
            this.startDateHour = dateParser.parse(parameters.getString(TweetPeaks.START_DATE, "0000-00-00-00"));
            this.endDateHour = dateParser.parse(parameters.getString(TweetPeaks.END_DATE, "00000-00-00-00"));
        } catch (ParseException ex) {
            LOG.fatal(ex.getMessage() + " at " + ex.getErrorOffset());
        }

        LOG.fatal("Peaks start:" + this.startDateHour + " end: " + this.endDateHour);
    }

    @Override
    public void reduce(Iterator<Record> hashtags, Collector<Record> records) throws Exception {
        Record pr = null;
        long hoursDiff = 0;
        int lastCount = 0;
        tempDateHour.setTime(startDateHour.getTime());
        nextDateHour.setTime(startDateHour.getTime());
        tempCount.setValue(0);


        // Iterate through existing time stamps
        while (hashtags.hasNext()) {
            pr = hashtags.next();
            // current times stamp
            tempDateHour = dateParser.parse(pr.getField(0, StringValue.class).getValue());


            // difference from expected timestamp
            hoursDiff = (tempDateHour.getTime() - nextDateHour.getTime()) / (60 * 60 * 1000);

            // prepare record
            prOut.setField(0, pr.getField(0, StringValue.class));
            prOut.setField(1, pr.getField(1, StringValue.class));


            // This is done only if Diff > 0
            // generated fakes time evaluations to fill holes
            for (int i = 0; i < hoursDiff; i++) {
                tempDateValue.setValue(dateParser.format(nextDateHour));
                prOut.setField(0, tempDateValue);
                lastCount /= 2;
                tempCount.setValue(lastCount);
                prOut.setField(2, tempCount);
                prOut.setField(3, isFake);

                if (TweetPeaks.ProduceTimeSeriesLog) {
                    this.fakeCounter++;
                }
                records.collect(prOut);

                // generate next expected timestamp
                nextDateHour.setTime(nextDateHour.getTime() + (60 * 60 * 1000));
            }
            lastCount = pr.getField(2, IntValue.class).getValue();
            tempCount.setValue(lastCount);
            prOut.setField(2, tempCount);
            prOut.setField(3, notFake);

            if (TweetPeaks.ProduceTimeSeriesLog) {
                this.trueCounter++;
            }
            records.collect(prOut);

            nextDateHour.setTime(nextDateHour.getTime() + (60 * 60 * 1000));

        }


        hoursDiff = 1 + (endDateHour.getTime() - nextDateHour.getTime()) / (60 * 60 * 1000);
        // This is done only if Diff > 0
        // generated fakes time evaluations
        for (int i = 0; i < hoursDiff; i++) {
            tempDateValue.setValue(dateParser.format(nextDateHour));
            prOut.setField(0, tempDateValue);
            lastCount /= 2;
            tempCount.setValue(lastCount);
            prOut.setField(2, tempCount);
            prOut.setField(3, isFake);

            if (TweetPeaks.ProduceTimeSeriesLog) {
                this.fakeCounter++;
            }
            records.collect(prOut);

            // generate next expected timestamp
            nextDateHour.setTime(nextDateHour.getTime() + (60 * 60 * 1000));
        }

    }

    @Override
    public void close() throws Exception {
        if (TweetPeaks.ProduceTimeSeriesLog) {
            LOG.fatal("True dates: " + this.trueCounter + " fake dates " + this.fakeCounter);
        }
        super.close();
    }
}
