module Amarcord.Chemical exposing (..)

import Amarcord.Attributo exposing (AttributoMap, AttributoValue, convertAttributoMapFromApi)
import Api.Data exposing (ChemicalType(..), JsonChemical, JsonFileOutput)
import Dict
import Json.Decode as Decode
import Json.Encode as Encode


chemicalTypeDecoder : Decode.Decoder ChemicalType
chemicalTypeDecoder =
    Decode.string
        |> Decode.andThen
            (\str ->
                case str of
                    "crystal" ->
                        Decode.succeed ChemicalTypeCrystal

                    "solution" ->
                        Decode.succeed ChemicalTypeSolution

                    _ ->
                        Decode.fail <| "unknown chemical type " ++ str
            )


chemicalTypeToString : ChemicalType -> String
chemicalTypeToString ct =
    case ct of
        ChemicalTypeCrystal ->
            "crystal"

        ChemicalTypeSolution ->
            "solution"


chemicalTypeToPrettyString : ChemicalType -> String
chemicalTypeToPrettyString ct =
    case ct of
        ChemicalTypeCrystal ->
            "Crystal"

        ChemicalTypeSolution ->
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


chemicalTypeToApi : ChemicalType -> Api.Data.ChemicalType
chemicalTypeToApi a =
    case a of
        ChemicalTypeCrystal ->
            Api.Data.ChemicalTypeCrystal

        ChemicalTypeSolution ->
            Api.Data.ChemicalTypeSolution


chemicalTypeFromApi : Api.Data.ChemicalType -> ChemicalType
chemicalTypeFromApi a =
    case a of
        Api.Data.ChemicalTypeCrystal ->
            ChemicalTypeCrystal

        Api.Data.ChemicalTypeSolution ->
            ChemicalTypeSolution


convertChemicalFromApi : JsonChemical -> Chemical ChemicalId (AttributoMap AttributoValue) JsonFileOutput
convertChemicalFromApi c =
    { id = c.id
    , name = c.name
    , type_ = chemicalTypeFromApi c.chemicalType
    , responsiblePerson = c.responsiblePerson
    , attributi = convertAttributoMapFromApi c.attributi
    , files = c.files
    }


chemicalTypeFromString : String -> ChemicalType
chemicalTypeFromString s =
    if s == "crystal" then
        ChemicalTypeCrystal

    else
        ChemicalTypeSolution
