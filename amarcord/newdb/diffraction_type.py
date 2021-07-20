import enum


class DiffractionType(enum.Enum):
    no_diffraction = "no diffraction"
    no_crystal = "no crystal"
    ice_salt = "ice / salt"
    success = "success"
