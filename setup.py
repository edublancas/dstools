from setuptools import setup

setup(name = 'dstools',
      packages = ['dstools'],
      version = '0.3',
      description='Utility functions for Data Science projects.',
      url='https://github.com/edublancas/dstools',
      download_url = 'https://github.com/edublancas/dstools/tarball/0.3',
      author = 'Eduardo Blancas Reyes',
      author_email = 'edu.blancas@gmail.com',
      license ='MIT',
      keywords = ['datascience'],
      classifiers = [],
      install_requires=[
          'pyaml',
      ]
)