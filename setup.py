import setuptools

setuptools.setup(
    name="amarcord-amici",
    version="0.2pre3",
    author="Philipp Middendorf <philipp.middendorf@desy.de>",
    author_email="sc@cfel.de",
    description="external interface for AMARCORD",
    packages=setuptools.find_packages(),
    install_requires=["sqlalchemy==1.3.*"],
    python_requires=">=3.8",
)
