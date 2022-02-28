module Amarcord.Sample exposing (..)

import Dict


type alias Sample idType attributiType fileType =
    { id : idType
    , name : String
    , attributi : attributiType
    , files : List fileType
    }


type alias SampleId =
    Int


sampleMapAttributi : (b -> c) -> Sample a b x -> Sample a c x
sampleMapAttributi f { id, name, attributi, files } =
    { id = id, name = name, attributi = f attributi, files = files }


sampleMapId : (a -> b) -> Sample a c x -> Sample b c x
sampleMapId f { id, name, attributi, files } =
    { id = f id, name = name, attributi = attributi, files = files }


sampleIdDict : List (Sample Int b c) -> Dict.Dict Int String
sampleIdDict =
    List.foldr (\s -> Dict.insert s.id s.name) Dict.empty
