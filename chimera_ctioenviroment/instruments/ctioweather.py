import logging
import time
import datetime

from astropy import units
from chimera.core.exceptions import OptionConversionException
from chimera.core.lock import lock
from chimera.instruments.weatherstation import WeatherBase
from chimera.interfaces.weatherstation import WSValue
import sqlalchemy


class CTIOWeather(WeatherBase):
    __config__ = {"model": "CTIO BLANCO telescope weather station",
                  "check_interval": 3 * 60,  # in seconds
                  "uri": "mysql://user:password@host/database/",
                  }

    def __init__(self):
        WeatherBase.__init__(self)
        self._last_check = 0
        self._time_ws = None

        # logging.
        # put every logger on behalf of chimera's logger so
        # we can easily setup levels on all our parts
        logName = self.__module__
        if not logName.startswith("chimera."):
            logName = "chimera." + logName + " (%s)" % logName

        self.log = logging.getLogger(logName)

    def __start__(self):
        self.engine = sqlalchemy.create_engine(self['uri'])

    def _get_mysql(self):
        '''
        Connect to the CTIO database and get the weather data.
        :return: Weather station raw measurements: date_time, temp, hum, wind_speed, wind_dir, pressure
        '''

        self.log.debug("Querying BLANCO meteo station...")
        try:
            connection = self.engine.connect()
        except Exception, e:
            self.log.error('Error connecting to URI %s: %s' % (self["uri"], e))
            return False

        result = connection.execute("select time, temp, hum, pres, wdir, wspeed"
                                    "  from weather"
                                    "  order by ID DESC"
                                    "  LIMIT 1")
        row = result.fetchone()

        connection.close()
        return row["TIME_WS"], row["WS_TEMP"], row["WS_HUMIDITY"], row["WS_WSPEED"], row["WS_WDIR"], row["WS_PRESSURE"]


    @lock
    def _check(self):
        if time.time() >= self._last_check + self["check_interval"]:
            try:
                time_ws, temp_out, hum_out, wind_speed, wind_dir, pressure = self._get_mysql()
            except TypeError:
                return False
            self._time_ws = time_ws
            self._temperature = float(temp_out)
            self._humidity = float(hum_out)
            self._wind_speed = float(wind_speed)
            self._wind_dir = float(wind_dir)
            self._pressure = float(pressure)
            self._last_check = time.time()
            return True
        else:
            return True

    def obs_time(self):
        ''' Returns a string with UT date/time of the meteorological observation
        '''
        if self._time_ws is None:
            return None
        dt = datetime.datetime.strptime(self._time_ws, '%Y-%m-%dUT%H:%M:%S')
        return dt.strftime("%Y-%m-%dT%H:%M:%S.%f")

    def humidity(self, unit_out=units.pct):

        if unit_out not in self.__accepted_humidity_units__:
            raise OptionConversionException("Invalid humidity unit %s." % unit_out)

        if self._check():
            return WSValue(self.obs_time(), self._convert_units(self._humidity, units.pct, unit_out), unit_out)
        else:
            return False

    def temperature(self, unit_out=units.Celsius):

        if unit_out not in self.__accepted_temperature_units__:
            raise OptionConversionException("Invalid temperature unit %s." % unit_out)

        if self._check():
            return WSValue(self.obs_time(), self._convert_units(self._temperature, units.Celsius, unit_out), unit_out)
        else:
            return False

    def wind_speed(self, unit_out=units.meter / units.second):

        if self._check():
            return WSValue(self.obs_time(), self._convert_units(self._wind_speed, (units.m / units.s), unit_out),
                           unit_out)
        else:
            return False

    def wind_direction(self, unit_out=units.degree):

        if self._check():
            return WSValue(self.obs_time(), self._convert_units(self._wind_dir, units.deg, unit_out), unit_out)
        else:
            return False

    def dew_point(self, unit_out=units.Celsius):
        return NotImplementedError()

    def pressure(self, unit_out=units.Pa):
        if self._check():
            return WSValue(self.obs_time(), self._convert_units(self._pressure, units.cds.mmHg, unit_out), unit_out)
        else:
            return False

    def rain(self, unit_out=units.imperial.inch/units.hour):
        return NotImplementedError()

    def getMetadata(self, request):

        return [('ENVMOD', str(self['model']), 'Weather station Model'),
                ('ENVTEM', self.temperature(unit_out=units.deg_C).value, '[degC] Weather station temperature'),
                ('ENVHUM', self.humidity(unit_out=units.pct).value, '[%] Weather station relative humidity'),
                ('ENVWIN', self.wind_speed(unit_out=units.m / units.s).value, '[m/s] Weather station wind speed'),
                ('ENVDIR', self.wind_direction(unit_out=units.deg).value, '[deg] Weather station wind direction'),
                ('ENVPRE', self.pressure(unit_out=units.cds.mmHg).value, '[mmHg] Weather station air pressure'),
                ('ENVDAT', self.obs_time(), 'UT time of the meteo observation')
                ]


if __name__ == '__main__':
    test = CTIOWeather()
    test.__start__()
    print test.getMetadata(None)
