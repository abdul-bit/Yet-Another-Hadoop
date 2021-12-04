from setuptools import setup
setup(
    name = 'hdfs',
    version = '0.1.0',
    packages = ['hdfs'],
    entry_points = {
        'console_scripts': [
            'hdfs = hdfs.__main__:main'
        ]
    })