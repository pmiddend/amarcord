module Amarcord.HttpError exposing (..)

import Amarcord.Html exposing (div_, h5_, p_, strongText)
import Api exposing (sendWithCustomExpect)
import Html exposing (Html, em, h4, h5, kbd, pre, text)
import Html.Attributes exposing (class)
import Http
import Json.Decode as Decode


{-| Elm does provide Http.Error, but that type throws away lots of
information about the request. So we have our own error type, based on
Http.Response (which still has this information)

Specifically, we provide user-friendly detail error messages in the
JSON response to erroneous requests, as such:

    {"detail": "this request failed"}

Parsing this is integrated into this type as well.

-}
type HttpError
    = BadUrl String
    | Timeout
    | NetworkError
    | BadStatus Http.Metadata String
    | BadStatusWithDetailMessage Http.Metadata String
    | BadJson String


expectJson : (Result HttpError a -> msg) -> Decode.Decoder a -> Http.Expect msg
expectJson toMsg decoder =
    Http.expectStringResponse toMsg <|
        \response ->
            case response of
                Http.BadUrl_ url ->
                    Err (BadUrl url)

                Http.Timeout_ ->
                    Err Timeout

                Http.NetworkError_ ->
                    Err NetworkError

                Http.BadStatus_ metadata body ->
                    case Decode.decodeString (Decode.field "detail" Decode.string) body of
                        Err _ ->
                            Err (BadStatus metadata body)

                        Ok detailMessage ->
                            Err (BadStatusWithDetailMessage metadata detailMessage)

                Http.GoodStatus_ _ body ->
                    case Decode.decodeString decoder body of
                        Ok value ->
                            Ok value

                        Err err ->
                            Err (BadJson (Decode.errorToString err))


showError : HttpError -> Html msg
showError x =
    case x of
        BadUrl url ->
            h5 [ class "alert-heading" ] [ text ("Bad URL: " ++ url) ]

        Timeout ->
            h5 [ class "alert-heading" ] [ text "Timeout" ]

        NetworkError ->
            h5 [ class "alert-heading" ] [ text "Network error" ]

        BadStatus metadata content ->
            div_
                [ h5 [ class "alert-heading" ] [ text <| "Bad status: " ++ String.fromInt metadata.statusCode ]
                , p_ [ text "Error message: ", em [] [ text metadata.statusText ] ]
                , if String.isEmpty content then
                    text ""

                  else
                    div_ [ h5_ [ text "Response content" ], pre [] [ text content ] ]
                ]

        BadStatusWithDetailMessage metadata detailMessage ->
            div_
                [ h4 [ class "alert-heading" ] [ text <| "Bad status: " ++ String.fromInt metadata.statusCode ]
                , p_ [ strongText "Error message: ", em [] [ text detailMessage ] ]
                , p_ [ strongText "What can you do?", text " If you can't decipher what the error message is supposed to mean, then you can do nothing other than contact the admins with this error." ]
                ]

        BadJson detailMessage ->
            div_
                [ h5 [ class "alert-heading" ] [ text <| "Error decoding server response" ]
                , p_
                    [ text "This usually means you have to refresh your browser, clearing the cache. Typically this is done with the key combination "
                    , kbd [] [ text "Ctrl" ]
                    , text " + "
                    , kbd [] [ text "F5" ]
                    , text " or "
                    , kbd [] [ text "Ctrl" ]
                    , text " + "
                    , kbd [] [ text "Shift" ]
                    , text " + "
                    , kbd [] [ text "R" ]
                    , text ". If that doesn’t help, then we have a more serious problem and you should contact the admins."
                    ]
                , p_ [ text "The actual decoding error is as follows: ", pre [] [ text detailMessage ] ]
                ]


{-| This is then the preferred way to send request to the OpenAPI
endpoint. It uses our custom error type
-}
send : (Result HttpError a -> msg) -> Api.Request a -> Cmd msg
send toMsg =
    sendWithCustomExpect (expectJson toMsg)
