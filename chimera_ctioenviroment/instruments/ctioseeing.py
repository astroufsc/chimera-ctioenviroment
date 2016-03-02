import logging
import time
import datetime

from astropy import units
from chimera.core.exceptions import OptionConversionException
from chimera.core.lock import lock
import sqlalchemy

from chimera.interfaces.seeingmonitor import SeeingValue
from chimera.instruments.seeingmonitor import SeeingBase


class CTIOSeeing(SeeingBase):
    __config__ = {"model": "CTIO BLANCO seeing monitor",
                  "check_interval": 3 * 60,  # in seconds
                  "uri": "mysql://user:password@host/database/",
                  }

    def __init__(self):
        SeeingBase.__init__(self)
        self._last_check = 0
        self._time_sm = None

        # logging.
        # put every logger on behalf of chimera's logger so
        # we can easily setup levels on all our parts
        logName = self.__module__
        if not logName.startswith("chimera."):
            logName = "chimera." + logName + " (%s)" % logName

        self.log = logging.getLogger(logName)

        self.engine = sqlalchemy.create_engine(self['uri'])

    def __start__(self):
        pass

    def _get_mysql(self):
        '''
        Connect to the CTIO database and get the seeing data.
        :return: Weather station raw measurements: date_time, seeing
        '''

        self.log.debug("Querying BLANCO meteo station...")
        try:
            connection = self.engine.connect()
        except Exception, e:
            self.log.error('Error connecting to URI %s: %s' % (self["uri"], e))
            return False

        result = connection.execute("select datetime, see6pt from T3_dimm order by datetime desc limit 1")
        row = result.fetchone()

        connection.close()
        return row['datetime'], row['see6pt']


    @lock
    def _check(self):
        if time.time() >= self._last_check + self["check_interval"]:
            try:
                time_sm, seeing = self._get_mysql()
            except TypeError:
                return False
            self._time_sm = time_sm
            self._seeing = float(seeing)
            self._last_check = time.time()
            return True
        else:
            return True

    def obs_time(self):
        ''' Returns a string with UT date/time of the meteorological observation
        '''
        if self._time_sm is None:
            return None
        return self._time_sm.strftime("%Y-%m-%dT%H:%M:%S.%f")

    def getSeeing(self, unit=units.arcsec):

        if unit not in self.__accepted_seeing_units__:
            raise OptionConversionException("Invalid seeing unit %s." % unit)

        if self._check():
            return SeeingValue(self.obs_time(), self._convert_units(self._seeing, units.arcsec, unit), unit)
        else:
            return False

    def getMetadata(self, request):

        return [('SEEMOD', str(self['model']), 'Seeing monitor Model'),
                ('SEETYP', str(self['model']), 'Seeing monitor type'),
                ('SEEVAL', self.getSeeing(unit=units.arcsec).value, '[degC] Weather station temperature'),
                ('SEEDAT', self.obs_time(), 'UT time of the seeing observation')
                ]


if __name__ == '__main__':
    test = CTIOSeeing()
    print test.getMetadata(None)
