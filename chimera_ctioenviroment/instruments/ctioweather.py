import logging
import time
import datetime
import numpy as np
from astropy import units
from astropy.units import imperial
from chimera.core.exceptions import OptionConversionException
from chimera.core.lock import lock
from chimera.instruments.weatherstation import WeatherBase
from chimera.interfaces.weatherstation import WSValue, WeatherTemperature, WeatherHumidity, WeatherPressure, WeatherWind
import sqlalchemy



class CTIOWeather(WeatherBase, WeatherTemperature, WeatherHumidity, WeatherPressure, WeatherWind):
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
                                    "  order by time DESC"
                                    "  LIMIT 1")
        row = result.fetchone()

        connection.close()
        return row["time"], row["temp"], row["hum"], row["wspeed"], row["wdir"], row["pres"]

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
        return self._time_ws

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
            return WSValue(self.obs_time(), self._convert_units(self._wind_speed, (imperial.mile / units.hour), unit_out),
                           unit_out)
        else:
            return False

    def wind_direction(self, unit_out=units.degree):

        if self._check():
            return WSValue(self.obs_time(), self._convert_units(self._wind_dir, units.deg, unit_out), unit_out)
        else:
            return False

    def dew_point(self, unit_out=units.Celsius):
        '''
        Calculates dew point according to the  Arden Buck equation (https://en.wikipedia.org/wiki/Dew_point).

        :param unit_out:
        :return:
        '''

        b = 18.678
        c = 257.14  # Celsius
        d = 235.5  # Celsius

        gamma_m = lambda T, RH: np.log(RH / 100. * np.exp((b - T / d) * (T / (c + T))))
        Tdp = lambda T, RH: c * gamma_m(T, RH) / (b - gamma_m(T, RH))

        return WSValue(self.obs_time(),
                       self._convert_units(Tdp(self.temperature(units.deg_C).value,
                                               self.humidity(units.pct).value),
                                           units.Celsius, unit_out), unit_out)

    def pressure(self, unit_out=units.Pa):
        if self._check():
            return WSValue(self.obs_time(), self._convert_units(self._pressure, units.cds.mmHg, unit_out), unit_out)
        else:
            return False

    def getMetadata(self, request):

        return [('ENVMOD', str(self['model']), 'Weather station Model'),
                ('ENVTEM', self.temperature(unit_out=units.deg_C).value, '[degC] Weather station temperature'),
                ('ENVDEW', self.dew_point(unit_out=units.deg_C).value, '[degC] Weather station dew point temperature'),
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
