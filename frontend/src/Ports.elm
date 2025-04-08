port module Ports exposing (copyToClipboard, storeLocalStorage)


port storeLocalStorage : String -> Cmd msg


port copyToClipboard : String -> Cmd msg
