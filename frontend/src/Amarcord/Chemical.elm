module Amarcord.Chemical exposing (..)

import Dict
import Json.Decode as Decode
import Json.Encode as Encode


type ChemicalType
    = Crystal
    | Solution


chemicalTypeDecoder : Decode.Decoder ChemicalType
chemicalTypeDecoder =
    Decode.string
        |> Decode.andThen
            (\str ->
                case str of
                    "crystal" ->
                        Decode.succeed Crystal

                    "solution" ->
                        Decode.succeed Solution

                    _ ->
                        Decode.fail <| "unknown chemical type " ++ str
            )


chemicalTypeToString : ChemicalType -> String
chemicalTypeToString ct =
    case ct of
        Crystal ->
            "crystal"

        Solution ->
            "solution"


chemicalTypeToPrettyString : ChemicalType -> String
chemicalTypeToPrettyString ct =
    case ct of
        Crystal ->
            "Crystal"

        Solution ->
            "Solution"


encodeChemicalType : ChemicalType -> Encode.Value
encodeChemicalType =
    Encode.string << chemicalTypeToString


type alias Chemical idType attributiType fileType =
    { id : idType
    , name : String
    , responsiblePerson : String
    , type_ : ChemicalType
    , attributi : attributiType
    , files : List fileType
    }


type alias ChemicalId =
    Int


chemicalMapAttributi : (b -> c) -> Chemical a b x -> Chemical a c x
chemicalMapAttributi f { id, name, responsiblePerson, type_, attributi, files } =
    { id = id, name = name, responsiblePerson = responsiblePerson, type_ = type_, attributi = f attributi, files = files }


chemicalMapId : (a -> b) -> Chemical a c x -> Chemical b c x
chemicalMapId f { id, name, responsiblePerson, type_, attributi, files } =
    { id = f id, name = name, responsiblePerson = responsiblePerson, type_ = type_, attributi = attributi, files = files }


chemicalIdDict : List (Chemical Int b c) -> Dict.Dict Int String
chemicalIdDict =
    List.foldr (\s -> Dict.insert s.id s.name) Dict.empty
