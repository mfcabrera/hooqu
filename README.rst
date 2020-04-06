===============================
Hooqu - Unit Tests for Data
===============================

Jukumari (pronounced hooqumari) is the Aymara name for the spectacled bear, also known as the Andean
bear, Andean short-faced bear, or mountain bear. Other names include ukumari or ukuku.

Hooqu is a library built on top of Pandas-like dataframes for defining "unit tests for data", which measure data quality datasets. Hooqu is an "espiritual" Python port of `Apache Deequ <https://github.com/awslabs/deequ/>`_

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
 

.. code-block:: python

    import pandas as pd
   
    from hooqu.verification_suite import VerificationSuite
    from hooqu.checks import Check, CheckLevel, CheckStatus
    
    df = pd.util.testing.makeDataFrame() # Dataframe size = 30
   
    verification_result = (VerificationSuite()
    .on_data(df)
    .add_check(
        Check(_
            CheckLevel.ERROR,  
            "Basic Check"
        )
       .has_size(lambda x: x > 10)
       .has_min("A", lambda min_: min_ > -99)
       .has_min("X", lambda min_: min_ < -9999)
       ).run()
    )   

    if verification_result.status == CheckStatus.SUCCESS:
        print("Alles klar: The data passed the test, everything is fine!")
    else:
        print("We found errors in the data")

     


* Free software: Apache Software License 2.0
* Documentation: https://hooqu.readthedocs.io.


Features
--------

* TODO

Credits
---------

This package was created with Cookiecutter_ and the `audreyr/cookiecutter-pypackage`_ project template.
