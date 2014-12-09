This directory is pretty half-baked. It gives a few scripts helpful
for running edX scripts over clusters of machines. This is probably
the wrong way to do things, so you probably want to look elsewhere,
but if you're interested in where we are today (Dec 9, 14), there is a
script which will queue locations of all of the tracking logs into
Amazon SQS.

There is a second script which will pull from SQS, run an operation
over individual log files, and dump to a different S3 bucket.

This does a subset of what Hadoop does poorly, but it was faster to
hack together than getting Hadoop working correctly for this
particular application. Hadoop would be better. Storm would be even
better, and probably the right path forward.

So why this? At this point, I'm more interested in the functionality
than the APIs, and more interested in the APIs than the plumbing
behind them. Once the functionality is right, I'll clean up the
APIs. Once the APIs are right, I'll clean up the plumbing. In the
meantime, this gives a bit of scaffolding to work with.