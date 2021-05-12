# coding=utf-8
import datetime
import json
import re
import time
import requests
import xmltodict
from astropy import units
from chimera.core.exceptions import OptionConversionException
from chimera.instruments.weatherstation import WeatherBase
from chimera.interfaces.weatherstation import WeatherTransparency, WeatherTemperature, WeatherHumidity, WeatherPressure, \
    WeatherWind, WSValue
from chimera.util.image import ImageUtil
from requests.exceptions import ConnectTimeout, ReadTimeout, ConnectionError

wind_dir = {'E': 90.0, 'ENE': 67.5, 'ESE': 112.5, 'N': 0.0, 'NE': 45.0, 'NNE': 22.5, 'NNW': 337.5, 'NW': 315.0,
            'S': 180.0, 'SE': 135.0, 'SSE': 157.5, 'SSW': 202.5, 'SW': 225.0, 'W': 270.0, 'WNW': 292.5, 'WSW': 247.5}


def wind_direction(wind_dir_letters):
    """
    :param wind_dir_letters: Up to three letter wind direction. Example: NNW
    :return: angle: Wind angle in degrees.
    """
    try:
        direction = wind_dir[wind_dir_letters]
    except KeyError:
        direction = 0
    return direction


class LCOGTScrapper(object):
    """
    Web scrapper for the LCOGT telops page
    """

    def __init__(self):
        self.results = dict()

    def scrape(self):

        _base_url = "https://weather-api.lco.global/query?site=lsc&datumname="
        urls = {'humidity': _base_url + "Weather%20Humidity%20Value",
                'temperature': _base_url + "Weather%20Air%20Temperature%20Value",
                'wind_speed': _base_url + "Weather%20Wind%20Speed%20Value",
                'wind_direction': _base_url + "Weather%20Wind%20Direction%20Value",
                'dew_point': _base_url + "Weather%20Dew%20Point%20Value",
                'pressure': _base_url + "Weather%20Barometric%20Pressure%20Value",
                'sky_transparency': _base_url + "Boltwood%20Transparency%20Measure"
                }

        for key in urls.keys():
            try:
                self.results[key] = requests.get(urls[key]).json()[-1]
            except:
                self.results[key] = {u'TimeStamp': u'',
                                     u'TimeStampMeasured': u'',
                                     u'Value': None,
                                     u'ValueString': u''}

        return self.results


class LCOGTWeather(WeatherBase, WeatherTemperature, WeatherHumidity, WeatherPressure,
                   WeatherWind, WeatherTransparency):
    """
    Instrument that gets information from LCOGT web page
    """

    __config__ = dict(
        model="LCOGT weather",
    )

    def __start__(self):
        """
        Start a thread that will be querying the LCGOT forever and ever...
        """
        self.__stop = False
        self._results = None
        self._scrapper = LCOGTScrapper()
        self.setHz(1. / 120)

    def __stop__(self):
        self.__stop = True

    def _update(self, value):
        """
        Updates with the LCOGT results
        """
        if all([v in value.keys() for v in
                ['humidity', 'pressure', 'temperature', 'sky_transparency', 'dew_point',
                 'wind_speed', 'wind_direction']]):
            if not value.has_key('Interlock Reason'):
                value['Interlock Reason'] = ''
            self._results = value
            self._results['utctime'] = datetime.datetime.strptime(value['temperature']['TimeStamp'].replace('/', '-'), '%Y-%m-%d %H:%M:%S')
            # self.log.debug('Updated LCOGT data: ' + self._results.__str__())

    def control(self):
        """
        Watches LCOGT for data
        """

        if self.__stop:
            return True

        try:
            value = self._scrapper.scrape()
        except ConnectTimeout:
            self.log.warn('Timeout connecting to the weather server.')
            return True
        except ReadTimeout:
            self.log.warn('Timeout reading the weather server.')
            return True
        except ConnectionError:
            self.log.warn('Connection error.')
            return True

        if value is None:
            return True
        else:
            self._update(value)
            return True


    def humidity(self, unit_out=units.pct):

        if self._results is None:
            return False

        if unit_out not in self.__accepted_humidity_units__:
            raise OptionConversionException("Invalid humidity unit %s." % unit_out)
        
        return WSValue(self._results['utctime'],
                       self._convert_units(self._results['humidity']['Value'], units.pct, unit_out),
                       unit_out)

    def temperature(self, unit_out=units.Celsius):

        if self._results is None:
            return False

        if unit_out not in self.__accepted_temperature_units__:
            raise OptionConversionException("Invalid temperature unit %s." % unit_out)

        return WSValue(self._results['utctime'],
                       self._convert_units(self._results['temperature']['Value'],
                                           units.Celsius, unit_out),
                       unit_out)

    def wind_speed(self, unit_out=units.meter / units.second):

        if self._results is None:
            return False

        if unit_out not in self.__accepted_speed_units__:
            raise OptionConversionException("Invalid speed unit %s." % unit_out)

        return WSValue(self._results['utctime'],
                       self._convert_units(self._results['wind_speed']['Value'],
                                           (units.m / units.s), unit_out),
                       unit_out)

    def wind_direction(self, unit_out=units.degree):

        if self._results is None:
            return False

        if unit_out not in self.__accepted_direction_unit__:
            raise OptionConversionException("Invalid speed unit %s." % unit_out)

        return WSValue(self._results['utctime'],
                       self._convert_units(self._results['wind_direction']['Value'],
                                           units.deg, unit_out),
                       unit_out)

    def dew_point(self, unit_out=units.Celsius):
        """
        :param unit_out:
        :return:
        """

        if self._results is None:
            return False

        if unit_out not in self.__accepted_temperature_units__:
            raise OptionConversionException("Invalid dew point unit %s." % unit_out)

        return WSValue(self._results['utctime'],
                       self._convert_units(self._results['dew_point']['Value'],
                                           units.Celsius, unit_out),
                       unit_out)

    def pressure(self, unit_out=units.Pa):

        if self._results is None:
            return False

        if unit_out not in self.__accepted_pressures_unit__:
            raise OptionConversionException("Invalid pressure unit %s." % unit_out)

        return WSValue(self._results['utctime'],
                       self._convert_units(self._results['pressure']['Value'],
                                           units.cds.mmHg, unit_out),
                       unit_out)

    def sky_transparency(self, unit_out=units.pct):

        if self._results is None:
            return False

        if unit_out not in self.__accepted_transparency_unit__:
            raise OptionConversionException("Invalid sky transparency unit %s." % unit_out)

        return WSValue(self._results['utctime'],
                       self._convert_units(self._results['sky_transparency']['Value'],
                                           units.pct, unit_out),
                       unit_out)

    def getMetadata(self, request):

        try:
            return [('ENVMOD', str(self['model']), 'Weather station Model'),
                    ('ENVTEM', self.temperature(unit_out=units.deg_C).value, '[degC] Weather station temperature'),
                    ('ENVDEW', self.dew_point(unit_out=units.deg_C).value, '[degC] Weather station dew point temperature'),
                    ('ENVHUM', self.humidity(unit_out=units.pct).value, '[%] Weather station relative humidity'),
                    ('ENVWIN', self.wind_speed(unit_out=units.m / units.s).value, '[m/s] Weather station wind speed'),
                    ('ENVDIR', self.wind_direction(unit_out=units.deg).value, '[deg] Weather station wind direction'),
                    ('ENVPRE', self.pressure(unit_out=units.cds.mmHg).value, '[mmHg] Weather station air pressure'),
                    ('ENVDAT', ImageUtil.formatDate(self._results['utctime']), 'UT time of the meteo observation')
                    ]
        except AttributeError:
            return []


if __name__ == '__main__':
    # test = LCOGTScrapper()
    test = LCOGTWeather()
    time.sleep(10)
    test.__start__()
    test.control()
    print test.getMetadata(None)
