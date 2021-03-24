let
  pkgs = import <nixpkgs> {};
  pythonPkgs = pkgs.python3Packages;
  lark-parser = pythonPkgs.buildPythonPackage rec {
    pname = "lark-parser";
    version = "0.11.2";

    src = pythonPkgs.fetchPypi {
      inherit pname version;
      sha256 = "0sxvg09xrp68i2g79926v2nzjfcyhx4rsz9k5x847cpjxdhh8qgg";
    };

    doCheck = false;
    
    propagatedBuildInputs = [ ];
  };
  pubchempy = pythonPkgs.buildPythonPackage rec {
    pname = "PubChemPy";
    version = "1.0.4";

    src = pythonPkgs.fetchPypi {
      inherit pname version;
      sha256 = "03pdv1hhvkz7m480bbsgi000fhdi23jhby2bfsr57c8ar4pxrs94";
    };

    doCheck = false;
    
    propagatedBuildInputs = [ ];
  };
  amarcordPackage = pythonPkgs.buildPythonPackage rec {
  pname = "amarcord-xfel";
  version = "1.0";

  src = ./.;

  doCheck = false;
  
  propagatedBuildInputs = [
    lark-parser
    pythonPkgs.isodate
    pythonPkgs.sqlalchemy
    pythonPkgs.pyqt5
    pythonPkgs.pyyaml
    pythonPkgs.xdg
    pythonPkgs.pandas
    pythonPkgs.matplotlib
    pythonPkgs.humanize
    pythonPkgs.pint
    pubchempy
  ];

  dontWrapQtApps = true;
  nativeBuildInputs = [ pkgs.qt5.wrapQtAppsHook ];

  makeWrapperArgs = [
    "\${qtWrapperArgs[@]}"
  ];

  };
in 
pkgs.dockerTools.buildImage {
  name = "amarcord";
  tag = "latest";

  contents = amarcordPackage;

  config = {
    Cmd = [ "/bin/amarcord-xfel-gui" ];
  };
}
