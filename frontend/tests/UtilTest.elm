module UtilTest exposing (..)

import Amarcord.Util exposing (foldPairs, withLeftNeighbor)
import Expect
import Test exposing (..)


suite : Test
suite =
    describe "Util"
        [ test "fold pairs leaves out first element" <|
            \_ ->
                Expect.equal [ ( 2, 3 ), ( 1, 2 ) ] (foldPairs [ 1, 2, 3 ] (\( prior, current ) -> ( prior, current )))
        , test "with left neighbors works" <|
            \_ ->
                Expect.equal [ ( Nothing, 1 ), ( Just 1, 2 ), ( Just 2, 3 ) ] (withLeftNeighbor [ 1, 2, 3 ] (\prior current -> ( prior, current )))
        ]
