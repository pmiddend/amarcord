import setuptools

setuptools.setup(
    name="amarcord-amici",
    version="0.2pre6",
    author="Philipp Middendorf <philipp.middendorf@desy.de>",
    author_email="sc@cfel.de",
    description="external interface for AMARCORD",
    packages=setuptools.find_packages(),
    install_requires=["SQLAlchemy==1.3.*", "lark==0.11.*", "isodate==0.6.*"],
    extras_require={
        "gui": [
            "PyQt5==5.15.*",
            "PyQt5-stubs==5.15.*",
            "pyzmq==22.0.*",
            "PyYAML==5.4.*",
            "pandas==1.2.*",
            "PubChemPy==1.0.*",
            "humanize==3.2.*",
            "metadata-client==3.0.*",
            "PyYAML==5.4.*",
            "karabo-bridge==0.6.*",
            "numpy==1.19.*",
            "matplotlib==3.3.*",
            "pint==0.16.*",
            "xdg==5.0.*",
        ],
        "webserver": [
            "Flask==1.1.*",
            "Flask-Cors==3.0.*",
        ],
    },
    python_requires=">=3.8",
)
