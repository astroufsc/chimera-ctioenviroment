import json
import re
import threading
import datetime
import requests
import time
from astropy import units
from chimera.core.exceptions import OptionConversionException
from chimera.instruments.weatherstation import WeatherBase
import xmltodict
from chimera.interfaces.weatherstation import WeatherTransparency, WeatherTemperature, WeatherHumidity, WeatherPressure, \
    WeatherWind, WSValue, WeatherSafety

wind_dir = {'E': 90.0, 'ENE': 67.5, 'ESE': 112.5, 'N': 0.0, 'NE': 45.0, 'NNE': 22.5, 'NNW': 337.5, 'NW': 315.0,
            'S': 180.0, 'SE': 135.0, 'SSE': 157.5, 'SSW': 202.5, 'SW': 225.0, 'W': 270.0, 'WNW': 292.5, 'WSW': 247.5}


def wind_direction(wind_dir_letters):
    """
    :param wind_dir_letters: Up to three letter wind direction. Example: NNW
    :return: angle: Wind angle in degrees.
    """
    return wind_dir[wind_dir_letters]


class LCOGTScrapper(object):
    """
    Web scrapper for the LCOGT telops page
    """

    def scrape(self):
        client = requests.session()

        data = client.get('http://telops.lcogt.net/#')

        latest_comet_queue_id = int(re.findall('Telops.latest_comet_queue_id = (.+);', data.text)[0])

        r = client.post(
            url='http://telops.lcogt.net/dajaxice/netnode.refresh/',
            data={'argv': json.dumps({"latest": latest_comet_queue_id})},
            headers={
                'Accept': '*/*',
                'Accept-Encoding': 'gzip, deflate',
                "Content-Type": "application/x-www-form-urlencoded",
                'Host': 'telops.lcogt.net',
                "Origin": "http://telops.lcogt.net",
                "Referer": "http://telops.lcogt.net/",
                'X-CSRFToken': None,
                'X-Requested-With': 'XMLHttpRequest',

            },
            cookies={'pushstate': 'pushed'}

        )
        return json.loads(r.text)


class LCOGTWeather(WeatherBase, WeatherTemperature, WeatherHumidity, WeatherPressure, WeatherWind, WeatherTransparency,
                   WeatherSafety):
    """
    Instrument that gets information from LCOGT web page
    """

    __config__ = dict(
        model="LCOGT weather",
        poll_frequency=120  # in seconds
    )

    def __start__(self):
        """
        Start a thread that will be querying the LCGOT forever and ever...
        """
        self.__stop = False
        self._results = None
        self._scrapper = LCOGTScrapper()
        p = threading.Thread(target=self._watch)
        p.start()

    def __stop__(self):
        self.__stop = True

    def _update(self, value):
        """
        Updates with the LCOGT results
        """
        if all([v in self._results for v in
                ['Humidity', 'Pressure', 'Temperature', 'Brightness', 'Transparency', 'Dew Point', 'Wind',
                 'Interlock Reason']]) and value is not None:
            self._results = value
            self._results['utctime'] = datetime.datetime.strptime(value['utctime'], '%Y-%m-%d %H:%M UTC')
            self.log.debug('Updated LCOGT data: ' + self._results.__str__())

    def _watch(self):
        """
        Watches LCOGT for data
        """
        while 1:

            if self.__stop:
                return

            data = self._scrapper.scrape()
            value = None
            utctime = None

            for val in data:
                if 'id' in val.keys():
                    if val['id'] == '#site-lsc-time':
                        utctime = re.sub('.*<b>', '', val['val'])
                        utctime = re.sub('<\/b>', '', utctime)
                    if val['id'] == '#site-lsc-ssb-system-Weather-tip':
                        try:
                            value = dict([v.split(':') if isinstance(v, basestring) else [None, None] for v in
                                          xmltodict.parse(re.sub('<\/?td(.*?)>', '',
                                                                 val['val'].replace('&nbsp;', ' ').replace('<b>',
                                                                                                           '').replace(
                                                                     '</b>', '')))['div']['table']['tr']])
                            value.pop(None)
                            value[u'utctime'] = utctime
                            value[u'Wind Dir'] = wind_direction(value[u'Wind'].split(' ')[-1])
                            for k in value.keys():
                                if k in [u'Temperature', u'Brightness', u'Humidity', u'Pressure', u'Transparency',
                                         u'Dew Point', u'Wind']:
                                    value[k] = float(re.sub('(unknown)', 'nan', re.sub('[\xb0C %].*', '', value[k])))
                                if k == 'OK to open':
                                    value[k] = value[k] in ['True']
                        except TypeError:
                            self.log.debug('LCOGTWeather TypeError: ' + val['val'])

            if value is not None:
                self.log.debug('LCOGTWeather.value >> ' + value.__str__())
                self._update(value)

            for i in range(int(self['poll_frequency'])):
                if self.__stop:
                    return
                time.sleep(1)

    def humidity(self, unit_out=units.pct):

        if self._results is None:
            return False

        if unit_out not in self.__accepted_humidity_units__:
            raise OptionConversionException("Invalid humidity unit %s." % unit_out)

        return WSValue(self._results['utctime'], self._convert_units(self._results['Humidity'], units.pct, unit_out),
                       unit_out)

    def temperature(self, unit_out=units.Celsius):

        if self._results is None:
            return False

        if unit_out not in self.__accepted_temperature_units__:
            raise OptionConversionException("Invalid temperature unit %s." % unit_out)

        return WSValue(self._results['utctime'],
                       self._convert_units(self._results['Temperature'], units.Celsius, unit_out), unit_out)

    def wind_speed(self, unit_out=units.meter / units.second):

        if self._results is None:
            return False

        if unit_out not in self.__accepted_speed_units__:
            raise OptionConversionException("Invalid speed unit %s." % unit_out)

        return WSValue(self._results['utctime'],
                       self._convert_units(self._results['Wind'], (units.m / units.s), unit_out), unit_out)

    def wind_direction(self, unit_out=units.degree):

        if self._results is None:
            return False

        if unit_out not in self.__accepted_direction_unit__:
            raise OptionConversionException("Invalid speed unit %s." % unit_out)

        return WSValue(self._results['utctime'], self._convert_units(self._results['Wind Dir'], units.deg, unit_out),
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
                       self._convert_units(self._results['Dew Point'], units.Celsius, unit_out), unit_out)

    def pressure(self, unit_out=units.Pa):

        if self._results is None:
            return False

        if unit_out not in self.__accepted_pressures_unit__:
            raise OptionConversionException("Invalid pressure unit %s." % unit_out)

        return WSValue(self._results['utctime'],
                       self._convert_units(self._results['Pressure'], units.cds.mmHg, unit_out), unit_out)

    def sky_transparency(self, unit_out=units.pct):

        if self._results is None:
            return False

        if unit_out not in self.__accepted_transparency_unit__:
            raise OptionConversionException("Invalid sky transparency unit %s." % unit_out)

        return WSValue(self._results['utctime'],
                       self._convert_units(self._results['Transparency'], units.pct, unit_out), unit_out)

    def okToOpen(self):
        """
        Returns True always when sun is up or when sun is down and 'OK to open' is True.
        Use with care during the day!
        """
        return self._results['Interlock Reason'] == 'sun up' or self._results['OK to open']

    def getMetadata(self, request):

        return [('ENVMOD', str(self['model']), 'Weather station Model'),
                ('ENVTEM', self.temperature(unit_out=units.deg_C).value, '[degC] Weather station temperature'),
                ('ENVDEW', self.dew_point(unit_out=units.deg_C).value, '[degC] Weather station dew point temperature'),
                ('ENVHUM', self.humidity(unit_out=units.pct).value, '[%] Weather station relative humidity'),
                ('ENVWIN', self.wind_speed(unit_out=units.m / units.s).value, '[m/s] Weather station wind speed'),
                ('ENVDIR', self.wind_direction(unit_out=units.deg).value, '[deg] Weather station wind direction'),
                ('ENVPRE', self.pressure(unit_out=units.cds.mmHg).value, '[mmHg] Weather station air pressure'),
                ('ENVDAT', self._results['utctime'], 'UT time of the meteo observation')
                ]


if __name__ == '__main__':
    test = LCOGTWeather()
    time.sleep(10)
    print test.getMetadata(None)
