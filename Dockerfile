# Why ubuntu and not python? We need libxcb-util1 for PyQt5 (or rather, qt5) and Python, based on
# some old Debian image, doesn't support this.
FROM ubuntu:groovy
WORKDIR /app
RUN apt-get update
RUN apt-get install -y python3-pip libgl1 qt5-default libxcb-util1 python-is-python3
COPY . .
RUN pip install -e .[gui]
CMD [ "python", "amarcord-xfel/amarcord/xfel_gui.py" ]
