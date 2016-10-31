from setuptools import setup

setup(
    name='xanalytics',
    packages=['xanalytics',
              'xanalytics.gzipfs',
              'xcluster',
              'xresources',
              'cmd',
              'edxhelpers'],
    long_description="Random analytics scripts over edX data."
)
