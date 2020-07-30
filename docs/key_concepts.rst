Key Concepts in the Code base
============================

Adapted from this `this document
<https://raw.githubusercontent.com/awslabs/hooqu/master/docs/key-concepts.md>`_.
This document is is targeted towards advanced users or contributors. The concepts here
are not necessary to use ``hooqu``.

There are a few key concepts that will help you to understand the
code base.

Metrics, Analyzers, and State
-----------------------------

Metrics represent some metric associated with the data that changes over
time. For example counting the rows in a DataFrame.

An Analyzer knows how to calculate a Metric based on some input
DataFrame.

:class:`.State` is an optimization, it represents the state of the data, from
which a metric can be calculated. This intermediate state can then be
used to calculate future metrics more quickly. Check out the examples
for some further details (future).

Overall flow of running hooqu checks
------------------------------------

When running checks a user specifies a DataFrame and a number of checks
to do on that DataFrame. Many checks in Hooqu are based on metrics which
describe the data. In order to perform the checks the user requests
hooqu follows the following process:

- First hooqu figures out which Analyzers are required
- Metrics are calculated using those Analyzers
- Metrics are also stored if a MetricsRepository is provided (future)
- Checks are evaluated using the calculated Metrics

Analyzers
~~~~~~~~~

Types of analyzers:

- :class:`.ScanShareableAnalyzer`: an analyzer which computes a metric based
  on a straight scan over the data, without any grouping being
  required.
- :class:`.GroupingAnalyzer`: an analyzer that requires the data to be
  grouped by a set of columns before the metric can be calculated

Metrics
~~~~~~~

A :class:`.Metric` includes the following key details

- name: the name for the type of metric.
- entity: the type of entity the metric is recorded against. e.g. A
  column, dataset, or multicolumn.
- instance: information about this instance of the metric. For example
  this could be the column name the metric is operating on
- value: the value of the metric at a point in time. The type of this
  value varies between metrics.

Metrics storage
^^^^^^^^^^^^^^^

Metrics can be stored in a metrics repository. (not yet implemented)
An entry in the repository consists of:

- A ``resultKey``: The combination of a timestamp and a map of tags.
  Typically a user may want to record things like the data source
  (e.g. table name) with the tags.  The resultKey can be used to
  lookup stored metrics
- An :class:`.AnalyzerContext`, which consists of a map of :class:`.Analyzer` to :class:`.Metric`.
