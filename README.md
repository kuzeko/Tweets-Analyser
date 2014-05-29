Tweets-Analyser
===============

Stratosphere PACT Program to run statistics over Tweets

It parses tweets records, cleaning the texts, computing sentiment analysis and collecting statistics about words popularity evolution over time.

![Tweets-Analyser in action](https://raw.github.com/kuzeko/Tweets-Analyser/master/report/images/tweets-hour.png "Tweets per Hour")

![Tweets-Analyser in action](https://raw.github.com/kuzeko/Tweets-Analyser/master/report/images/boston-v-thatcher.png "Popularity Evolution of words Thatcher and Boston")


![Tweets-Analyser in action](https://raw.github.com/kuzeko/Tweets-Analyser/master/report/images/obama-sentiment.png "Sentiment Analysis")

It uses also a customized version of [SentiStrength](http://sentistrength.wlv.ac.uk/), which needs to be downloaded separately from the official website.
SentiStrength Data dir is also needed, and the flow should be configured accordingly.


Configuration is present in the maven file.
