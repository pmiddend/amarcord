port module Ports exposing (changeTitle, copyToClipboard, storeLocalStorage)


port storeLocalStorage : String -> Cmd msg


port copyToClipboard : String -> Cmd msg


port changeTitle : String -> Cmd msg
