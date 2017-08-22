import logging
import time
from astropy import units
from chimera.core.exceptions import OptionConversionException
from chimera.core.lock import lock
import sqlalchemy
from chimera.interfaces.seeingmonitor import SeeingValue
from chimera.instruments.seeingmonitor import SeeingBase


class CTIOSeeing(SeeingBase):
    __config__ = {"model": "CTIO BLANCO seeing monitor - DIMM2",
                  "type": "DIMM",
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

    def __start__(self):
        self.engine = sqlalchemy.create_engine(self['uri'], pool_recycle=3600)

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

        result = connection.execute("select ut, seeing, airmass, flux_s1, flux_s2"
                                    "  from DIMM2_SEEING"
                                    " order by ut desc"
                                    " limit 1")
        row = result.fetchone()

        connection.close()
        return row['ut'], float(row['seeing']), float(row['airmass']), float(row['flux_s1']) + float(row['flux_s2'])

    @lock
    def _check(self):
        if time.time() >= self._last_check + self["check_interval"]:
            try:
                self._time_sm, self._seeing, self._airmass, self._flux = self._get_mysql()
            except TypeError:
                return False
            self._last_check = time.time()
            return True
        else:
            return True

    def obs_time(self):
        '''
        Returns a string with UT date/time of the meteorological observation
        '''
        if self._time_sm is None:
            return None
        return self._time_sm

    def seeing(self, unit=units.arcsec):

        if unit not in self.__accepted_seeing_units__:
            raise OptionConversionException("Invalid seeing unit %s." % unit)

        if self._check():
            return SeeingValue(self.obs_time(), self._convert_units(self._seeing, units.arcsec, unit), unit)
        else:
            return False

    def airmass(self, unit=units.dimensionless_unscaled):

        if unit not in self.__accepted_airmass_units__:
            raise OptionConversionException("Invalid airmass unit %s." % unit)

        if self._check():
            return SeeingValue(self.obs_time(), self._convert_units(self._airmass, units.dimensionless_unscaled, unit),
                               unit)
        else:
            return False

    def flux(self, unit=units.count):

        if unit not in self.__accepted_flux_units__:
            raise OptionConversionException("Invalid flux unit %s." % unit)

        if self._check():
            return SeeingValue(self.obs_time(), self._convert_units(self._flux, units.count, unit), unit)
        else:
            return False

    def getMetadata(self, request):

        if not self._check():
            return 

        return [('SEEMOD', str(self['model']), 'Seeing monitor Model'),
                ('SEETYP', str(self['type']), 'Seeing monitor type'),
                ('SEEVAL', self.seeing(unit=units.arcsec).value, '[arcsec] Seeing value'),
                ('SEEFLU', self.flux(unit=units.count).value, '[counts] Star flux value'),
                ('SEEDAT', self.obs_time().strftime("%Y-%m-%dT%H:%M:%S.%f"), 'UT time of the seeing observation')
                ]


if __name__ == '__main__':
    test = CTIOSeeing()
    print test.getMetadata(None)
