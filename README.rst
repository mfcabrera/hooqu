===============================
Hooqu - Unit Tests for Data
===============================

Jukumari (pronounced hooqumari) is the Aymara name for the spectacled bear, also known as the Andean
bear, Andean short-faced bear, or mountain bear. Other names include ukumari or ukuku.

Hooqu is a library built on top of Pandas-like dataframes for defining "unit tests for data", which measure data quality datasets. Hooqu is a "spiritual" Python port of `Apache Deequ <https://github.com/awslabs/deequ/>`_

I am happy to receive feedback and contributions.


.. image:: https://img.shields.io/pypi/v/hooqu.svg
        :target: https://pypi.python.org/pypi/hooqu

.. image:: https://img.shields.io/travis/mfcabrera/hooqu.svg
        :target: https://travis-ci.org/mfcabrera/hooqu

.. image:: https://readthedocs.org/projects/hooqu/badge/?version=latest
        :target: https://hooqu.readthedocs.io/en/latest/?badge=latest
        :alt: Documentation Status

.. image:: https://pyup.io/repos/github/mfcabrera/hooqu/shield.svg
     :target: https://pyup.io/repos/github/mfcabrera/hooqu/
     :alt: Updates

Sample code working so far:

.. code-block:: python

    import pandas as pd

    from hooqu.checks import Check, CheckLevel, CheckStatus
    from hooqu.constraints import ConstraintStatus
    from hooqu.verification_suite import VerificationSuite

    df = pd.util.testing.makeDataFrame()  # Dataframe size = 30

   verification_result = (
       VerificationSuite()
       .on_data(df)
       .add_check(
            Check(CheckLevel.ERROR, "Basic Check")
            .has_size(lambda x: x > 100)  # DataFrame should have more than 100 rows
            .has_min("A", lambda min_: min_ > -99)  # min(A) should be larger than -99
            .is_complete("B")  # should never be null
       )
      .run()
  )


    if verification_result.status == CheckStatus.SUCCESS:
        print("Alles klar: The data passed the test, everything is fine!")
    else:
        print("We found errors in the data")


    for check_result in verification_result.check_results.values():
        for cr in check_result.constraint_results:
            if cr.status != ConstraintStatus.SUCCESS:
                print(f"{cr.constraint}: {cr.message}")




* Free software: Apache Software License 2.0
* Documentation: https://hooqu.readthedocs.io.


Features
--------

* TODO
