from distutils.core import setup

setup(
    name='chimera_ctioenviroment',
    version='0.0.1',
    packages=['chimera_ctioenviroment', 'chimera_ctioenviroment.instruments'],
    scripts=[],
    requires=['_mysql'],
    url='http://github.com/astroufsc/chimera-ctioenviroment',
    license='GPL v2',
    author='William Schoenell',
    author_email='william@iaa.es',
    description='Chimera plugin for the CTIO weather station and seeing monitor'
)
