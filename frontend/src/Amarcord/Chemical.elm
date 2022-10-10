module Amarcord.Chemical exposing (..)

import Dict


type alias Chemical idType attributiType fileType =
    { id : idType
    , name : String
    , attributi : attributiType
    , files : List fileType
    }


type alias ChemicalId =
    Int


chemicalMapAttributi : (b -> c) -> Chemical a b x -> Chemical a c x
chemicalMapAttributi f { id, name, attributi, files } =
    { id = id, name = name, attributi = f attributi, files = files }


chemicalMapId : (a -> b) -> Chemical a c x -> Chemical b c x
chemicalMapId f { id, name, attributi, files } =
    { id = f id, name = name, attributi = attributi, files = files }


chemicalIdDict : List (Chemical Int b c) -> Dict.Dict Int String
chemicalIdDict =
    List.foldr (\s -> Dict.insert s.id s.name) Dict.empty
