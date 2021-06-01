module App.Components.Router where

import Prelude

import App.API (AnalysisColumn(..))
import App.AppMonad (AppMonad)
import App.Components.Analysis as Analysis
import App.Components.Beamline as Beamline
import App.Halogen.FontAwesome (icon)
import App.HalogenUtils (classList, singleClass)
import App.Root as Root
import App.Route (Route(..), routeCodec, sameRoute)
import App.SortOrder (SortOrder(..))
import Data.Either (hush)
import Data.Maybe (Maybe(..), fromMaybe)
import Data.Symbol (SProxy(..))
import Effect.Class (class MonadEffect, liftEffect)
import Effect.Console (log)
import Halogen as H
import Halogen.HTML as HH
import Halogen.HTML.Events as HE
import Halogen.HTML.Properties as HP
import Halogen.HTML.Properties.ARIA as HPA
import Routing.Duplex (parse, print)
import Routing.Hash (getHash, setHash)
import Web.Event.Event (preventDefault)
import Web.UIEvent.MouseEvent (MouseEvent, toEvent)

amarcordProgramVersion :: String
amarcordProgramVersion = "latest"

type State
  = { route :: Maybe Route
    }

data Query a
  = Navigate Route a

data Action
  = Initialize
  | GoTo Route MouseEvent

type OpaqueSlot slot
  = forall query. H.Slot query Void slot

type ChildSlots
  = ( root :: OpaqueSlot Unit
    , beamline :: OpaqueSlot Unit
    , analysis :: OpaqueSlot Unit
    )

component :: forall i o. H.Component HH.HTML Query i o AppMonad
component =
  H.mkComponent
    { initialState: const { route: Nothing }
    , render
    , eval:
        H.mkEval
          $ H.defaultEval
              { handleAction = handleAction
              , handleQuery = handleQuery
              , initialize = Just Initialize
              }
    }

handleQuery :: forall a o m. Query a -> H.HalogenM State Action ChildSlots o m (Maybe a)
handleQuery = case _ of
  -- This is the case that runs every time the brower's hash route changes.
  Navigate route a -> do
    mRoute <- H.gets _.route
    when (mRoute /= Just route)
      $ H.modify_ _ { route = Just route }
    pure (Just a)

navigate :: forall t30. MonadEffect t30 => Route -> t30 Unit
navigate = liftEffect <<< setHash <<< print routeCodec

handleAction :: forall o. Action -> H.HalogenM State Action ChildSlots o AppMonad Unit
handleAction = case _ of
  -- Handles initialization of the route
  Initialize -> do
    initialRoute <- hush <<< (parse routeCodec) <$> H.liftEffect getHash
    navigate $ fromMaybe Root initialRoute
  --  Handles the consecutive route changes.
  GoTo route e -> do
    liftEffect $ preventDefault (toEvent e)
    mRoute <- H.gets _.route
    when (mRoute /= Just route)
      $ do
          liftEffect $ log ("new route" <> print routeCodec route)
          navigate route

-- Renders a page component depending on which route is matched.
render :: State -> H.ComponentHTML Action ChildSlots AppMonad
render st =
  skeleton (st.route)
    $ case st.route of
        Nothing -> HH.h1_ [ HH.text "Oh no! That page wasn't found!" ]
        Just route -> case route of
          Root -> HH.slot (SProxy :: _ "root") unit Root.component unit absurd
          Analysis input -> HH.slot (SProxy :: _ "analysis") unit Analysis.component input absurd
          Beamline input -> HH.slot (SProxy :: _ "beamline") unit Beamline.component input absurd

navItems ::
  Array
    { fa :: String
    , link :: Route
    , title :: String
    }
navItems =
  [ { title: "P11"
    , link: Beamline { puckId: Nothing }
    , fa: "radiation"
    }
  , { title: "Analysis"
    , link: Analysis { sortOrder: Descending, sortColumn: AnalysisTime }
    , fa: "table"
    }
  ]

makeNavItem ::
  forall t158 t163.
  Maybe Route ->
  { fa :: String
  , link :: Route
  , title :: String
  | t158
  } ->
  HH.HTML t163 Action
makeNavItem route ({ title, link, fa }) =
  let
    isActive = if fromMaybe false (sameRoute link <$> route) then [ "active" ] else []
  in
    HH.li
      [ classList [ "nav-item" ] ]
      [ HH.a
          [ HP.href "#", HE.onClick (Just <<< GoTo link), classList ([ "nav-link" ] <> isActive) ]
          [ icon { name: fa, size: Nothing, spin: false }, HH.text (" " <> title) ]
      ]

navbar :: forall t222. Maybe Route -> HH.HTML t222 Action
navbar route =
  let
    header =
      HH.div
        [ classList [ "text-center", "p-3", "logo" ] ]
        [ HH.img [ HP.src "desy-cfel.png", HP.alt "DESY and CFEL logo combined", classList [ "img-fluid", "pb-3", "pt-3" ] ]
        , HH.h2 [ classList [ "logo-h2" ] ] [ HH.text "amarcord" ]
        , HH.small
            [ classList [ "text-muted" ] ]
            [ HH.text amarcordProgramVersion ]
        ]

    navMenu =
      HH.div [ classList [ "sidebar-sticky", "pt-3" ] ]
        [ HH.ul [ classList [ "nav", "flex-column" ] ]
            (makeNavItem route <$> navItems)
        ]
  in
    HH.nav
      [ classList [ "col-md-2", "col-lg-1", "d-md-block", "sidebar", "collapse" ]
      ]
      [ HH.div [ classList [ "position-sticky" ] ]
          [ header, navMenu ]
      ]

contentView :: forall t150 t151. HH.HTML t151 t150 -> HH.HTML t151 t150
contentView html =
  HH.main [ HPA.role "main", classList [ "col-md-10", "ml-sm-auto", "col-lg-11", "px-md-4", "pt-3" ] ]
    [ html ]

skeleton :: forall a. Maybe Route -> HH.HTML a Action -> HH.HTML a Action
skeleton route html =
  HH.main_
    [ HH.div [ classList [ "container" ] ]
        [ HH.header [ classList [ "d-flex", "flex-wrap", "justify-content-center", "py-3", "mb-4", "border-bottom" ] ]
            [ HH.img [ HP.src "desy-cfel.png", HP.alt "DESY and CFEL logo combined", classList [ "img-fluid", "amarcord-logo" ] ]
            , HH.a
                [ singleClass "d-flex align-items-center mb-3 mb-md-0 me-md-auto text-dark text-decoration-none" ]
                [ HH.span [ singleClass "fs-4" ] [ HH.text "AMARCORD" ] ]
            , HH.ul [ singleClass "nav nav-pills" ] (makeNavItem route <$> navItems)
            ]
        ]
    , HH.div [ singleClass "container" ]
        [ contentView html
        ]
    ]
