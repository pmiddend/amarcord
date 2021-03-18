import setuptools

setuptools.setup(
    name="amarcord-amici",
    version="0.2pre5",
    author="Philipp Middendorf <philipp.middendorf@desy.de>",
    author_email="sc@cfel.de",
    description="external interface for AMARCORD",
    packages=setuptools.find_packages(),
    install_requires=["SQLAlchemy==1.3.*", "lark==0.11.*"],
    python_requires=">=3.8",
)
