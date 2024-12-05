module Amarcord.MultiDict exposing
    ( MultiDict
    , empty, insert, update
    , member, get
    , foldr
    )

{-| A dictionary mapping unique keys to **multiple** values, allowing for
modelling **one-to-many relationships.**

Example usage:

    oneToMany : MultiDict String Int
    oneToMany =
        MultiDict.empty
            |> MultiDict.insert "A" 1
            |> MultiDict.insert "B" 2
            |> MultiDict.insert "C" 3
            |> MultiDict.insert "A" 2

    MultiDict.get "A" oneToMany
    --> Set.fromList [1, 2]

This module in particular uses [`assoc-list`](https://package.elm-lang.org/packages/pzp1997/assoc-list/latest/) and [`assoc-set`](https://package.elm-lang.org/packages/erlandsona/assoc-set/latest/)
under the hood to get rid of the `comparable` constraint on keys that's usually
associated with Dicts and Sets.


# Dictionaries

@docs MultiDict


# Differences from Dict


# Build

@docs empty, insert, update


# Query

@docs member, get


# Lists


# Transform

@docs foldr


# Combine

-}

import AssocList as Dict exposing (Dict)
import AssocSet as Set exposing (Set)


{-| The underlying data structure. Think about it as

     type alias MultiDict a b =
         Dict a (Set b) -- just a normal Dict!

-}
type MultiDict a b
    = MultiDict (Dict a (Set b))


{-| Create an empty dictionary.
-}
empty : MultiDict a b
empty =
    MultiDict Dict.empty


{-| Insert a key-value pair into a dictionary. Replaces value when there is
a collision.
-}
insert : a -> b -> MultiDict a b -> MultiDict a b
insert from to (MultiDict d) =
    MultiDict <|
        Dict.update
            from
            (\maybeSet ->
                case maybeSet of
                    Nothing ->
                        Just (Set.singleton to)

                    Just set ->
                        Just (Set.insert to set)
            )
            d


{-| Update the value of a dictionary for a specific key with a given function.
-}
update : a -> (Set b -> Set b) -> MultiDict a b -> MultiDict a b
update from fn (MultiDict d) =
    MultiDict <| Dict.update from (Maybe.andThen (normalizeSet << fn)) d


{-| In our model, (Just Set.empty) has the same meaning as Nothing.
Make it be Nothing!
-}
normalizeSet : Set a -> Maybe (Set a)
normalizeSet set =
    if Set.isEmpty set then
        Nothing

    else
        Just set


{-| Determine if a key is in a dictionary.
-}
member : a -> MultiDict a b -> Bool
member from (MultiDict d) =
    Dict.member from d


{-| Get the value associated with a key. If the key is not found, return
`Nothing`. This is useful when you are not sure if a key will be in the
dictionary.

    animals = fromList [ ("Tom", Cat), ("Jerry", Mouse) ]

    get "Tom"   animals == Just Cat
    get "Jerry" animals == Just Mouse
    get "Spike" animals == Nothing

-}
get : a -> MultiDict a b -> Set b
get from (MultiDict d) =
    Dict.get from d
        |> Maybe.withDefault Set.empty


{-| Fold over the key-value pairs in a dictionary from highest key to lowest key.


    getAges users =
        Dict.foldr addAge [] users

    addAge _ user ages =
        user.age :: ages

    -- getAges users == [28,19,33]

-}
foldr : (a -> Set b -> acc -> acc) -> acc -> MultiDict a b -> acc
foldr fn zero (MultiDict d) =
    Dict.foldr fn zero d
