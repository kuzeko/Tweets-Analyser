package eu.unitn.disi.db.eventer.reduce;

import eu.stratosphere.api.java.record.functions.FunctionAnnotation;
import eu.stratosphere.api.java.record.functions.ReduceFunction;
import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.types.IntValue;
import eu.stratosphere.types.Record;
import eu.stratosphere.types.StringValue;
import eu.stratosphere.util.Collector;
import eu.unitn.disi.db.eventer.TweetPeaks;
import eu.unitn.disi.db.eventer.utils.Peaks;
import eu.unitn.disi.db.eventer.utils.Peaks.Peak;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * For each hashtag counts the total number of tweets
 *
 * 0 - word
 * 1 - peak start
 * 2 - peak top
 * 3 - peak end
 * 4 - day of peak
 */
@FunctionAnnotation.ConstantFields({})
public class DetectPeaksReduce extends ReduceFunction {

    private static final Log LOG = LogFactory.getLog(DetectPeaksReduce.class);
    private long counter = 0;
    private double minAmplitude;
    private double minSlope;
    private int minSamples;
    private double smoothing;
    private Record prOut = new Record(5);
    private StringValue hashtag = new StringValue();
    private StringValue start = new StringValue();
    private StringValue top = new StringValue();
    private StringValue end = new StringValue();
    private StringValue peakDate = new StringValue();

    /**
     * Reads the filter literals from the configuration.
     *
     * @see
     * eu.stratosphere.pact.common.stubs.Function#open(eu.stratosphere.nephele.configuration.Configuration)
     */
    @Override
    public void open(Configuration parameters) {
        this.minAmplitude = Double.parseDouble(parameters.getString(TweetPeaks.MIN_AMPLITUDE, "0.2"));
        this.minSlope = Double.parseDouble(parameters.getString(TweetPeaks.MIN_SLOPE, "0.2"));
        this.minSamples = Integer.parseInt(parameters.getString(TweetPeaks.MIN_SAMPLES, "1"));
        this.smoothing = Double.parseDouble(parameters.getString(TweetPeaks.SMOOTHING, "0.2"));
        //LOG.fatal("Peaks parameters:" + " minAmp " + this.minAmplitude + " minSlope " + this.minSlope + " minSamples " + this.minSamples + " smoothing " + this.smoothing);
    }

    @Override
    public void reduce(Iterator<Record> hashtags, Collector<Record> records) throws Exception {
        Record pr = null;
        List<Double> t = new ArrayList<Double>();
        List<Double> v = new ArrayList<Double>();
        List<String> dates = new ArrayList<String>();

        double nextInstant = 0;
        double lastCount = 0;

        t.clear();
        v.clear();
        dates.clear();
        int countFake = 0;
        while (hashtags.hasNext()) {
            pr = hashtags.next();

            lastCount = pr.getField(2, IntValue.class).getValue();
            countFake += pr.getField(3, IntValue.class).getValue();
            t.add(nextInstant);
            v.add(lastCount);

            dates.add(pr.getField(0, StringValue.class).getValue());
            nextInstant++;
        }

        hashtag = pr.getField(1, StringValue.class);

        if (t.size() > 2 && v.size() > 2) {
            double overallAvg = 0;
            double actualAvg = 0;
            double[] plainV = getPlainArray(v);
            for (int i = 0; i < plainV.length; i++) {
                overallAvg += plainV[i];
            }
            actualAvg = overallAvg / (plainV.length- countFake) ;
            overallAvg /= plainV.length;

            if(actualAvg > this.minSamples) {

                if (TweetPeaks.DetectPeaksReduceLog) {
                    DecimalFormat df = new DecimalFormat("#####.###");
                    LOG.fatal("AVG for # " + hashtag + " " + df.format(overallAvg) + " and " + df.format(actualAvg) +  " over "+ plainV.length + " Data points of which fake "+ countFake);
                }


                List<Peak> peaks = Peaks.extractPeaks(getPlainArray(t), getPlainArray(v), minAmplitude, minSlope, minSamples, smoothing);


                for (Peak peak : peaks) {

                    start.setValue(dates.get(peak.getPeakBegin()));
                    top.setValue(dates.get(peak.getPeakTop()));
                    end.setValue(dates.get(peak.getPeakEnd()));
                    peakDate.setValue(dates.get(peak.getPeakTop()).substring(0, 10));

                    prOut.setField(0, hashtag);
                    prOut.setField(1, start);
                    prOut.setField(2, top);
                    prOut.setField(3, end);
                    prOut.setField(4, peakDate);
                    records.collect(prOut);

                    if (TweetPeaks.DetectPeaksReduceLog) {
                        this.counter++;
                    }

                }
            }
        }

        t.clear();
        v.clear();


    }

    private static double[] getPlainArray(List<Double> toConvert) {
        double[] converted = new double[toConvert.size()];
        int i = 0;
        for (Double d : toConvert) {
            converted[i++] = d.doubleValue();
        }
        return converted;
    }

    @Override
    public void close() throws Exception {
        if (TweetPeaks.DetectPeaksReduceLog) {
            LOG.fatal(counter);
        }
        super.close();
    }
}
