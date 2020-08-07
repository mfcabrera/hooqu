===============================
Hooqu - Unit Tests for Data
===============================

.. image:: https://img.shields.io/pypi/v/hooqu.svg
        :target: https://pypi.python.org/pypi/hooqu
.. image:: https://travis-ci.com/mfcabrera/hooqu.svg?token=pq89mpsBBBTg11hAgCHH&branch=master
        :target: https://travis-ci.com/mfcabrera/hooqu
.. image:: https://readthedocs.org/projects/hooqu/badge/?version=latest
        :target: https://hooqu.readthedocs.io/en/latest/?badge=latest
        :alt: Documentation Status
.. image:: https://codecov.io/gh/mfcabrera/hooqu/branch/master/graph/badge.svg
  :target: https://codecov.io/gh/mfcabrera/hooqu
.. image:: https://pyup.io/repos/github/mfcabrera/hooqu/shield.svg
     :target: https://pyup.io/repos/github/mfcabrera/hooqu/
     :alt: Updates

----------

**Documentation**: https://hooqu.readthedocs.io

**Source Code**: https://github.com/mfcabrera/hooqu

----------


Install
-------

Hooqu requires Pandas >= 1.0 and Python >= 3.7. To install via pip use:

::

   pip install hooqu


Quick Start
-----------


.. code:: python

   import pandas as pd

   # data to validate
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

Checks we want to perform:

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


If we run the example, we get the following output:

::

   We found errors in the data
   CompletenessConstraint(Completeness(productName)): Value 0.8 does not meet the constraint requirement.

The test found that our assumptions are violated! Only 4 out of 5 (80%) of the values of the productName attribute are non-null.
Fortunately, we ran a test and found the errors, somebody should immediately fix the data :)


Contributing
------------

All contributions, bug reports, bug fixes, documentation improvements,
enhancements and ideas are welcome.  Please use `GitHub issues
<https://github.com/mfcabrera/hooqu/issues>`_: for bug reports,
feature requests, install issues, RFCs, thoughts, etc.

See the full `cotributing guide <https://github.com/mfcabrera/hooqu/blob/master/CONTRIBUTING.rst>`_ for more information.


Why Hooqu?
----------

- Easy to use declarative API to add data verification steps to your
  data processing pipeline.
- The ``VerificationResult`` allows you know not only what check fail
  but the values of the computed metric, allowing for flexible
  handling of issues with the data.
- Incremental metric computation capability allows to compare quality
  metrics change across time (planned).
- Support for storing and loading computed metrics (planned).



References
----------

This project is a "spiritual" port of `Apache Deequ <https://github.com/awslabs/deequ/>`_ and thus tries to implement
the declarative API described on the paper "`Automating large-scale data quality verification <http://www.vldb.org/pvldb/vol11/p1781-schelter.pdf>`_"
while trying to remain pythonic as much as possible. This project does not use (py)Spark but rather
Pandas (and hopefully in the future it will support other compatible dataframe implementations).


Name
----

Jukumari (pronounced hooqumari) is the Aymara name for the `spectacled bear <https://en.wikipedia.org/wiki/Spectacled_bear>`_ (*Tremarctos ornatus*), also known as the Andean
bear, Andean short-faced bear, or mountain bear.
