from setuptools import setup

setup(name='ntfdl',
      version='0.0.1',
      description='Download free financial data from Netfonds ASA',
      url='http://github.com/ntftrader/ntfdl',
      author='Ntftrader',
      author_email='ntftrader@gmail.com',
      license='MIT',
      packages=['ntfdl'],
      install_requires=['pandas', 'bs4'],
      zip_safe=False)
