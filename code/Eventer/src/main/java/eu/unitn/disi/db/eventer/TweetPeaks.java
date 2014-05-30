package eu.unitn.disi.db.eventer;

import eu.stratosphere.api.common.Plan;
import eu.stratosphere.api.common.Program;
import eu.stratosphere.api.common.ProgramDescription;
import eu.stratosphere.api.common.operators.FileDataSink;
import eu.stratosphere.api.common.operators.FileDataSource;
import eu.stratosphere.api.common.operators.Order;
import eu.stratosphere.api.common.operators.Ordering;
import eu.stratosphere.api.java.record.io.CsvInputFormat;
import eu.stratosphere.api.java.record.io.CsvOutputFormat;
import eu.stratosphere.api.java.record.operators.JoinOperator;
import eu.stratosphere.api.java.record.operators.ReduceOperator;
import eu.stratosphere.types.DoubleValue;
import eu.stratosphere.types.IntValue;
import eu.stratosphere.types.StringValue;
import eu.unitn.disi.db.eventer.join.AssignHashtagsIdsJoin;
import eu.unitn.disi.db.eventer.join.HourlyTotalCountJoin;
import eu.unitn.disi.db.eventer.reduce.CollectPeaksReduce;
import eu.unitn.disi.db.eventer.reduce.DetectPeaksReduce;
import eu.unitn.disi.db.eventer.reduce.DetectProportionalPeaksReduce;
import eu.unitn.disi.db.eventer.reduce.ProduceTimeSeriesReduce;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

/**
 * Perform peeks detection
 * @author Davide Mottin <mottin@disi.unitn.eu>
 */
public class TweetPeaks implements Program, ProgramDescription {


    public static final String MIN_AMPLITUDE = "parameter.MIN_AMPLITUDE";
    public static final String MIN_SLOPE = "parameter.MIN_SLOP";
    public static final String MIN_SAMPLES = "parameter.MIN_SAMPLES";
    public static final String SMOOTHING = "parameter.SMOOTHING";
    public static final String START_DATE = "parameter.START_DATE";
    public static final String END_DATE = "parameter.END_DATE";

    /*
     * Profiling variables
     */
    public static final boolean AssignHashtagsIdsJoinLog           = true;
    public static final boolean HourlyTotalCountJoinLog            = true;
    public static final boolean ProduceTimeSeriesLog               = true;
    public static final boolean DetectPeaksReduceLog               = true;
    public static final boolean DetectProportioanlPeaksReduceLog   = true;
    public static final boolean CollectContemporaryPeaksLog        = true;




    @Override
    public Plan getPlan(String... args) {
        int noSubTasks           = (args.length > 0 ? Integer.parseInt(args[0]) : 1);
        String hashtagsIDs       = (args.length > 1 ? args[1] : "");
        String hashtagsHourly    = (args.length > 2 ? args[2] : "");
        String wordsHourly       = (args.length > 3 ? args[3] : "");
        String bigramsHourly     = (args.length > 4 ? args[4] : "");
        String trigramsHourly    = (args.length > 5 ? args[5] : "");
        String tweetsHourly      = (args.length > 6 ? args[6] : "");

        String minAmplitude      = (args.length > 7 ? args[7] : "0.2");
        String minSlope          = (args.length > 8 ? args[8] : "0.2");
        String minSamples        = (args.length > 9 ? args[9] : "1");
        String smoothing         = (args.length > 10 ? args[10] : "0.9");

        String startDateHour     = (args.length > 11 ? args[11] : "0000-00-00-00");
        String endDateHour       = (args.length > 12 ? args[12] : "0000-00-00-00");

        String outDir            = (args.length > 13 ? args[13] : "file:///tmp/");

        //String outputHashtagTimes      = outDir +"/hashtag_times";
        //String outputHashtagPeaks      = outDir +"/hashtag_peaks";
        String outputWordPeaks         = outDir +"/word_peaks";
        String outputWordPropPeaks     = outDir +"/word_prop_peaks";
        String outputBigramPeaks       = outDir +"/bigram_peaks";
        String outputTrigramPeaks      = outDir +"/trigram_peaks";
        String outputDailyPeaks        = outDir +"/daily_peaks";



        /*
         * Load Data
         */
        //FileDataSource hashtagsIds  = new FileDataSource(new CsvInputFormat('\t', IntValue.class, StringValue.class), hashtagsIDs, "Input Hashtags Id  mapping");
        //FileDataSource hashtags     = new FileDataSource(new CsvInputFormat('\t', StringValue.class, IntValue.class, IntValue.class), hashtagsHourly, "Input Hashtags per Hour");
        FileDataSource words        = new FileDataSource(new CsvInputFormat('\t', StringValue.class, StringValue.class, IntValue.class), wordsHourly, "Input Words per Hour");
        FileDataSource bigrams      = new FileDataSource(new CsvInputFormat('\t', StringValue.class, StringValue.class, IntValue.class), bigramsHourly, "Input Bigrams per Hour");
        FileDataSource trigrams     = new FileDataSource(new CsvInputFormat('\t', StringValue.class, StringValue.class, IntValue.class), trigramsHourly, "Input Trigrams per Hour");
        FileDataSource tweetsCount  = new FileDataSource(new CsvInputFormat('\t', StringValue.class, IntValue.class), tweetsHourly, "Input Tweets per Hour");

        /*
         * Flow
         */

        /*
         * Hashtags
         */
//        JoinOperator assignHashtagIds = JoinOperator.builder(AssignHashtagsIdsJoin.class, IntValue.class, 0, 1)
//                .input1(hashtagsIds)
//                .input2(hashtags)
//                .name("Assign Hashtag name")
//                .build();


//        ReduceOperator hashtagTimeSeries = ReduceOperator.builder(ProduceTimeSeriesReduce.class, StringValue.class, 1)
//                .secondaryOrder(new Ordering(0, StringValue.class, Order.ASCENDING))
//                .input(assignHashtagIds)
//                .name("Produce Hashtag  Time Series")
//                .build();
//        hashtagTimeSeries.setParameter(START_DATE, startDateHour);
//        hashtagTimeSeries.setParameter(END_DATE, endDateHour);


//        ReduceOperator detectHashtagPeaks = ReduceOperator.builder(DetectPeaksReduce.class, StringValue.class, 1)
//                .secondaryOrder(new Ordering(0, StringValue.class, Order.ASCENDING))
//                .input(hashtagTimeSeries)
//                .name("Detect Hashtag Peaks")
//                .build();
//        detectHashtagPeaks.setParameter(MIN_AMPLITUDE, minAmplitude);
//        detectHashtagPeaks.setParameter(MIN_SLOPE, minSlope);
//        detectHashtagPeaks.setParameter(MIN_SAMPLES, minSamples);
//        detectHashtagPeaks.setParameter(SMOOTHING, smoothing);



        /*
         * Words
         */
        ReduceOperator wordsTimeSeries = ReduceOperator.builder(ProduceTimeSeriesReduce.class, StringValue.class, 1)
                .secondaryOrder(new Ordering(0, StringValue.class, Order.ASCENDING))
                .input(words)
                .name("Produce Words  Time Series")
                .build();
        wordsTimeSeries.setParameter(START_DATE, startDateHour);
        wordsTimeSeries.setParameter(END_DATE, endDateHour);


        JoinOperator assignTotalTweetCounts = JoinOperator.builder(HourlyTotalCountJoin.class, StringValue.class, 0, 0)
            .input1(wordsTimeSeries)
            .input2(tweetsCount)
            .name("Join with Hourly Tweets Counts")
            .build();

        ReduceOperator detectWordPeaks = ReduceOperator.builder(DetectPeaksReduce.class, StringValue.class, 1)
                .secondaryOrder(new Ordering(0, StringValue.class, Order.ASCENDING))
                .input(wordsTimeSeries)
                .name("Detect Words Peaks")
                .build();
        detectWordPeaks.setParameter(MIN_AMPLITUDE, minAmplitude);
        detectWordPeaks.setParameter(MIN_SLOPE, minSlope);
        detectWordPeaks.setParameter(MIN_SAMPLES, minSamples);
        detectWordPeaks.setParameter(SMOOTHING, smoothing);

        ReduceOperator detectProportionalWordPeaks = ReduceOperator.builder(DetectProportionalPeaksReduce.class, StringValue.class, 1)
                .secondaryOrder(new Ordering(0, StringValue.class, Order.ASCENDING))
                .input(assignTotalTweetCounts)
                .name("Detect Proportioanl Words Peaks")
                .build();
        detectProportionalWordPeaks.setParameter(MIN_AMPLITUDE, minAmplitude);
        detectProportionalWordPeaks.setParameter(MIN_SLOPE, minSlope);
        detectProportionalWordPeaks.setParameter(MIN_SAMPLES, minSamples);
        detectProportionalWordPeaks.setParameter(SMOOTHING, smoothing);


        /*
         * Bigrams
         */
        ReduceOperator bigramsTimeSeries = ReduceOperator.builder(ProduceTimeSeriesReduce.class, StringValue.class, 1)
                .secondaryOrder(new Ordering(0, StringValue.class, Order.ASCENDING))
                .input(bigrams)
                .name("Produce Bigrams  Time Series")
                .build();
        bigramsTimeSeries.setParameter(START_DATE, startDateHour);
        bigramsTimeSeries.setParameter(END_DATE, endDateHour);


        ReduceOperator detectBigramPeaks = ReduceOperator.builder(DetectPeaksReduce.class, StringValue.class, 1)
                .secondaryOrder(new Ordering(0, StringValue.class, Order.ASCENDING))
                .input(bigramsTimeSeries)
                .name("Detect Bigrams Peaks")
                .build();
        detectBigramPeaks.setParameter(MIN_AMPLITUDE, minAmplitude);
        detectBigramPeaks.setParameter(MIN_SLOPE, minSlope);
        detectBigramPeaks.setParameter(MIN_SAMPLES, minSamples);
        detectBigramPeaks.setParameter(SMOOTHING, smoothing);

        /*
         * Trigrams
         */
        ReduceOperator trigramsTimeSeries = ReduceOperator.builder(ProduceTimeSeriesReduce.class, StringValue.class, 1)
                .secondaryOrder(new Ordering(0, StringValue.class, Order.ASCENDING))
                .input(trigrams)
                .name("Produce Trigram  Time Series")
                .build();
        trigramsTimeSeries.setParameter(START_DATE, startDateHour);
        trigramsTimeSeries.setParameter(END_DATE, endDateHour);


        ReduceOperator detectTrigramPeaks = ReduceOperator.builder(DetectPeaksReduce.class, StringValue.class, 1)
                .secondaryOrder(new Ordering(0, StringValue.class, Order.ASCENDING))
                .input(trigramsTimeSeries)
                .name("Detect Trigram Peaks")
                .build();
        detectTrigramPeaks.setParameter(MIN_AMPLITUDE, minAmplitude);
        detectTrigramPeaks.setParameter(MIN_SLOPE, minSlope);
        detectTrigramPeaks.setParameter(MIN_SAMPLES, minSamples);
        detectTrigramPeaks.setParameter(SMOOTHING, smoothing);


        ReduceOperator collectPeaks = ReduceOperator.builder(CollectPeaksReduce.class, StringValue.class, 4)
                .keyField(StringValue.class, 0)
                //.input(detectHashtagPeaks, detectWordPeaks, detectBigramPeaks, detectTrigramPeaks  )
                .input(detectWordPeaks, detectBigramPeaks, detectTrigramPeaks  )
                .name("Collect Daily peaks")
                .build();


        /*
         * Output
         */

        List<FileDataSink> outputs = new ArrayList<FileDataSink>();
        int i = 0;

//        outputs.add(new FileDataSink(new CsvOutputFormat(), outputHashtagTimes, hashtagTimeSeries, "Hashtag TimeSeries Produced"));
//        CsvOutputFormat.configureRecordFormat(outputs.get(i))
//                .recordDelimiter('\n')
//                .fieldDelimiter('\t')
//                .lenient(true)
//                .field(StringValue.class, 0)
//                .field(StringValue.class, 1)
//                .field(IntValue.class, 2)
//                .field(IntValue.class, 3);
//
//        i++;


//        outputs.add(new FileDataSink(new CsvOutputFormat(), outputHashtagPeaks, detectHashtagPeaks, "Hashtag Peaks Detected"));
//        outputs.get(i).setLocalOrder(new Ordering(0, StringValue.class, Order.ASCENDING));
//        CsvOutputFormat.configureRecordFormat(outputs.get(i))
//                .recordDelimiter('\n')
//                .fieldDelimiter('\t')
//                .lenient(true)
//                .field(StringValue.class, 0)
//                .field(StringValue.class, 1)
//                .field(StringValue.class, 2)
//                .field(StringValue.class, 3)
//                .field(DoubleValue.class, 5);
//
//        i++;
        outputs.add(new FileDataSink(new CsvOutputFormat(), outputWordPeaks, detectWordPeaks, "Words Peaks Detected"));
        outputs.get(i).setLocalOrder(new Ordering(0, StringValue.class, Order.ASCENDING));
        CsvOutputFormat.configureRecordFormat(outputs.get(i))
                .recordDelimiter('\n')
                .fieldDelimiter('\t')
                .lenient(true)
                .field(StringValue.class, 0)
                .field(StringValue.class, 1)
                .field(StringValue.class, 2)
                .field(StringValue.class, 3)
                .field(DoubleValue.class, 5);

        i++;
        outputs.add(new FileDataSink(new CsvOutputFormat(), outputWordPropPeaks, detectProportionalWordPeaks, "Words Proportional Peaks Detected"));
        outputs.get(i).setLocalOrder(new Ordering(0, StringValue.class, Order.ASCENDING));
        CsvOutputFormat.configureRecordFormat(outputs.get(i))
                .recordDelimiter('\n')
                .fieldDelimiter('\t')
                .lenient(true)
                .field(StringValue.class, 0)
                .field(StringValue.class, 1)
                .field(StringValue.class, 2)
                .field(StringValue.class, 3)
                .field(DoubleValue.class, 5);

        i++;
        outputs.add(new FileDataSink(new CsvOutputFormat(), outputBigramPeaks, detectBigramPeaks, "Bigram Peaks Detected"));
        outputs.get(i).setLocalOrder(new Ordering(0, StringValue.class, Order.ASCENDING));
        CsvOutputFormat.configureRecordFormat(outputs.get(i))
                .recordDelimiter('\n')
                .fieldDelimiter('\t')
                .lenient(true)
                .field(StringValue.class, 0)
                .field(StringValue.class, 1)
                .field(StringValue.class, 2)
                .field(StringValue.class, 3)
                .field(DoubleValue.class, 5);

        i++;
        outputs.add(new FileDataSink(new CsvOutputFormat(), outputTrigramPeaks, detectTrigramPeaks, "Trigram Peaks Detected"));
        outputs.get(i).setLocalOrder(new Ordering(0, StringValue.class, Order.ASCENDING));
        CsvOutputFormat.configureRecordFormat(outputs.get(i))
                .recordDelimiter('\n')
                .fieldDelimiter('\t')
                .lenient(true)
                .field(StringValue.class, 0)
                .field(StringValue.class, 1)
                .field(StringValue.class, 2)
                .field(StringValue.class, 3)
                .field(DoubleValue.class, 5);

        i++;
        outputs.add(new FileDataSink(new CsvOutputFormat(), outputDailyPeaks, collectPeaks, "Daily Peaks Collected"));
        outputs.get(i).setLocalOrder(new Ordering(0, StringValue.class, Order.ASCENDING));
        CsvOutputFormat.configureRecordFormat(outputs.get(i))
                .recordDelimiter('\n')
                .fieldDelimiter('\t')
                .lenient(true)
                .field(StringValue.class, 0)
                .field(StringValue.class, 1)
                .field(IntValue.class, 2);

        i++;




        HashSet outputsSet = new HashSet<FileDataSink>();
        outputsSet.addAll(outputs);

        Plan plan = new Plan(outputsSet, "Peaks Statistics Process");

        plan.setDefaultParallelism(noSubTasks);
        return plan;
    }

    @Override
    public String getDescription() {
        return "Parameters: [No Tasks] [HashtagsIDs] [HashtagHourly]" +
               " [WordsHourly] [BigramsHourly] [TrigramsHourly]" +
               " [minAmplitude] [minSlope] [minSamples] [smoothing]" +
               " [startDateHour] [endDateHour]" +
               " [outputDir]" ;
    }
}

