chimera-ctioenviroment plugin
=============================

This is a template plugin for the chimera observatory control system
https://github.com/astroufsc/chimera.

**Note:** Some features of this plugin works only inside the Cerro Tololo installations in La Serena, Chile.

.. image:: docs/screenshot.png


Installation
------------

::

    sudo apt-get install mysql-client libmysqlclient-dev  # for Ubuntu
    pip install MySQL-python
    pip install -U git+https://github.com/astroufsc/chimera-ctioenviroment.git


Configuration Example
---------------------

Here goes an example of the configuration to be added on ``chimera.config`` file.

::

    weatherstation:
        name: blanco
        type: CTIOWeather
        check_interval: 60  # minimum time between 2 queries. in seconds.
        uri: mysql://user:password@host/database/  # Database credentials.

    seeingmonitor:
        name: blanco
        type: CTIOSeeing
        check_interval:  60  # minimum time between 2 queries. in seconds.
        uri: mysql://user:password@host/database/  # Database credentials.

    # RASICAM cloud coverage:
    # http://rasicam.ctio.noao.edu/
    weatherstation:
        name: rasicam
        type: RASICAM

    # LCOGT webpage scrapper
    # http://telops.lcogt.net/
    weatherstation:
        name: lcogt
        type: LCOGTWeather

Acknowledgement
---------------

We thank LCOGT and DES/RASICAM project for sharing their weather data freely on the internet. We are very grateful for that.

Contact
-------

For more information, contact us on chimera's discussion list:
https://groups.google.com/forum/#!forum/chimera-discuss

Bug reports and patches are welcome and can be sent over our GitHub page:
https://github.com/astroufsc/chimera-ctioenviroment/
