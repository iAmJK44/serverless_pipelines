from setuptools import setup, find_packages


setup(name='Lithops-METASPACE',
      version='1.0.0',
      description='Lithops-powered METASPACE annotation pipeline',
      url='https://github.com/metaspace2020/Lithops-METASPACE',
      packages=find_packages(),
      install_requires=[
          "lithops==3.0.1",
          # pandas version should match the version in the runtime to ensure data generated locally can be unpickled
          # in Lithops actions
          "pandas",
          "pyarrow",
          "scipy",
          "pyImagingMSpec",
          "cpyImagingMSpec",
          "pyMSpec",
          "cpyMSpec==0.3.5",
          "pyimzML",
          "requests",
          "msgpack",
          "msgpack-numpy",
          "pypng",  # 0.0.20 introduced incompatible API changes
          "metaspace2020",  # Only needed for experiment notebooks
          "psutil",  # Only needed for experiment notebooks
          "cloudpickle"
      ])
