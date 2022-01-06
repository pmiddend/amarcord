module App.Formatting where

import Data.DateTime (DateTime)
import Data.Either (Either, either)
import Data.Formatter.DateTime (FormatterCommand(..), format, unformat)
import Data.Formatter.Number as NumberFormat
import Data.Function (const, (<<<))
import Data.List (List(..), (:))
import Data.Maybe (maybe)
import Data.Number.Format (fixed, toStringWith)
import Data.String (Pattern(..), lastIndexOf, take)

prettyPrintIsoDateTime :: String -> String
prettyPrintIsoDateTime v = either (const v) prettyPrintExternalDate (parseExternalDate v)

printDateTimeInIso :: DateTime -> String
printDateTimeInIso = format externalDateFormat

externalDateFormat :: List FormatterCommand
externalDateFormat =
  YearFull : Placeholder "-" : MonthTwoDigits : Placeholder "-" : DayOfMonthTwoDigits : Placeholder "T" : Hours24
    : Placeholder ":"
    : MinutesTwoDigits
    : Placeholder ":"
    : SecondsTwoDigits
    : Nil

parseExternalDate :: String -> Either String DateTime
parseExternalDate = unformat externalDateFormat <<< cutLastDot
  where
  cutLastDot x = maybe x (\i -> take i x) (lastIndexOf (Pattern ".") x)

prettyPrintExternalDate :: DateTime -> String
prettyPrintExternalDate =
  format
    ( YearFull : Placeholder "/" : MonthTwoDigits : Placeholder "/" : DayOfMonthTwoDigits : Placeholder " " : Hours24 : Placeholder ":" : MinutesTwoDigits : Placeholder ":" : SecondsTwoDigits
        : Nil
    )

floatNumberToString :: Number -> String
floatNumberToString = toStringWith (fixed 2)

intToString :: Number -> String
intToString =
  NumberFormat.format
    ( NumberFormat.Formatter
        { abbreviations: false, after: 0, before: 0, comma: true, sign: false
        }
    )
