===============================
Hooqu - Unit Tests for Data
===============================

Hooqu is a library built on top of Pandas dataframes for defining "unit tests for data",
which measure data quality datasets.
Hooqu is a "spiritual" Python port of `Apache Deequ <https://github.com/awslabs/deequ/>`_ and
is currently in an experimental state. I am happy to receive feedback and contributions.

Hooqu's purpose is to "unit-test" data to find errors early, before
the data gets fed to consuming systems or machine learning
algorithms. Note that "unit test" refers to the fact that the quality
of the data is being tested rather than to the software practice of
unit testing.  Hooqu is meant to be used as run-time check done during
a data processing/ingestion step.

Most applications that work with data have implicit assumptions about
that data, e.g. that attributes have certain types, do not contain
*NULL* values, and so on.

If these assumptions are violated, your application or machine
learning algorithm might crash or produce wrong outputs.

The idea behind Hooqu is to **explicitly state these assumptions** in the form of a
"unit-test" for data, which can be verified on a piece of data at
hand. If the data has errors, we can "quarantine" and fix it, before
we feed to an application or machine learning algorithm.


Usage Example
-------------

In the following, we will walk you through a toy example to showcase
the most basic usage of our library.  Hooqu works on tabular data,
e.g., CSV files, database tables, logs, flattened json files,
basically anything that you can fit into a Pandas dataframe.  For this
example, we assume that we work on some kind of Item data, where every
item has an id, a productName, a description, a priority and a count
of how often it has been viewed. Let's generate a toy example with few
records:

.. code:: python

   import pandas as pd

   df = pd.DataFrame(
          [
              (1, "Thingy A", "awesome thing.", "high", 0),
              (2, "Thingy B", "available at http://thingb.com", None, 0),
              (3, None, None, "low", 5),
              (4, "Thingy D", "checkout https://thingd.ca", "low", 10),
              (5, "Thingy E", None, "high", 12),
          ],
          columns=["id", "productName", "description", "priority", "numViews"]
   )


The main entry point for defining how you expect your data to look is
the `VerificationSuite
<https://hooqu.readthedocs.io/en/latest/hooqu.html#hooqu.verification_suite.VerificationSuite>`_
from which you can add `Checks
<https://hooqu.readthedocs.io/en/latest/hooqu.html#module-hooqu.checks>`_
that define constraints on attributes of the data. In this example, we
test for the following properties of our data:

- there are 5 rows in total
- values of the id attribute are never Null/None and unique
- values of the productName attribute are never null/None
- the priority attribute can only contain "high" or "low" as value
- numViews should not contain negative values
- at least half of the values in description should contain a url
- the median of numViews should be less than or equal to 10

In code this looks as follows:

.. code:: python

    from hooqu.checks import Check, CheckLevel, CheckStatus
    from hooqu.verification_suite import VerificationSuite
    from hooqu.constraints import ConstraintStatus


    verification_result = (
          VerificationSuite()
          .on_data(df)
          .add_check(
              Check(CheckLevel.ERROR, "Basic Check")
              .has_size(lambda sz: sz == 5)  # we expect 5 rows
              .is_complete("id")  # should never be None/Null
              .is_unique("id")  # should not contain duplicates
              .is_complete("productName")  # should never be None/Null
              .is_contained_in("priority", ("high", "low"))
              .is_non_negative("numViews")
              # .contains_url("description", lambda d: d >= 0.5) (NOT YET IMPLEMENTED)
              .has_quantile("numViews", 0.5, lambda v: v <= 10)
          )
          .run()
    )


After calling ``run``, hooqu will compute some metrics on the data. Afterwards it invokes your assertion functions
(e.g., ``lambda sz: sz == 5`` for the size check) on these metrics to see if the constraints hold on the data.
We can inspect the `VerificationResult <https://github.com/mfcabrera/hooqu/blob/b2c522854c674db9496c89d540df3fe4bb30d882/hooqu/verification_suite.py#L17>`_ to see if the test found errors:

.. code:: python

    if verification_result.status == CheckStatus.SUCCESS:
          print("Alles klar: The data passed the test, everything is fine!")
    else:
          print("We found errors in the data")

    for check_result in verification_result.check_results.values():
          for cr in check_result.constraint_results:
              if cr.status != ConstraintStatus.SUCCESS:
                  print(f"{cr.constraint}: {cr.message}")



.. toctree::
  :maxdepth: 2

  installation
  contributing
  history

API Reference
-------------

If you are looking for information on a specific function, class or
method, this part of the documentation is for you.

.. toctree::
  :maxdepth: 2

  Hooqu API Documentation <hooqu>



Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
