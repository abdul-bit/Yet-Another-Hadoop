from setuptools import setup
setup(
    name = 'yah',
    version = '0.1.0',
    packages = ['yah'],
    entry_points = {
        'console_scripts': [
            'yah = yah.__main__:main'
        ]
    })