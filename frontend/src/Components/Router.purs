module App.Components.Router where

import App.Components.Analysis as Analysis
import App.Components.Beamline as Beamline
import App.Components.Compounds as Compounds
import App.Components.Crystals as Crystals
import App.Components.JobList as JobList
import App.Components.Pucks as Pucks
import App.Components.SingleJob as SingleJob
import App.Components.Targets as Targets
import App.Components.P11Ingest as P11Ingest
import App.Components.Tools as Tools
import App.Components.ToolsAdmin as ToolsAdmin
import App.Halogen.FontAwesome (icon)
import App.HalogenUtils (classList, singleClass)
import App.Root as Root
import App.Route (Route(..), SelectedColumns(..), routeCodec, sameRoute, createLink)
import App.SortOrder (SortOrder(..))
import Control.Bind (discard, pure, when)
import Control.Monad (bind)
import Data.Either (hush)
import Data.Eq ((/=))
import Data.Foldable (any)
import Data.Function (const, ($), (<<<))
import Data.Functor ((<$>))
import Data.Maybe (Maybe(..), fromMaybe)
import Data.Semigroup ((<>))
import Data.Unit (Unit, unit)
import Data.Void (Void, absurd)
import Effect.Aff.Class (class MonadAff)
import Effect.Class (class MonadEffect)
import Halogen (liftEffect)
import Halogen as H
import Halogen.HTML as HH
import Halogen.HTML.Events as HE
import Halogen.HTML.Properties as HP
import Halogen.HTML.Properties.ARIA as HPA
import Routing.Duplex (parse, print)
import Routing.Hash (getHash, setHash)
import Type.Proxy (Proxy(..))
import Web.Event.Event (preventDefault)
import Web.UIEvent.MouseEvent (MouseEvent, toEvent)

amarcordProgramVersion :: String
amarcordProgramVersion = "latest"

type State
  =
  { route :: Maybe Route
  }

data Query a
  = Navigate Route a

data Action
  = Initialize
  | GoTo Route MouseEvent

type OpaqueSlot slot
  = forall query. H.Slot query Void slot

type ChildSlots
  =
  ( root :: OpaqueSlot Unit
  , crystals :: OpaqueSlot Unit
  , p11ingest :: OpaqueSlot Unit
  , pucks :: OpaqueSlot Unit
  , targets :: OpaqueSlot Unit
  , beamline :: OpaqueSlot Unit
  , compounds :: OpaqueSlot Unit
  , analysis :: OpaqueSlot Unit
  , tools :: OpaqueSlot Unit
  , toolsadmin :: OpaqueSlot Unit
  , jobs :: OpaqueSlot Unit
  , job :: OpaqueSlot Unit
  )

component :: forall i o m. MonadAff m => H.Component Query i o m
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

handleAction :: forall o m. MonadEffect m => Action -> H.HalogenM State Action ChildSlots o m Unit
handleAction = case _ of
  -- Handles initialization of the route
  Initialize -> do
    initialRoute <- hush <<< (parse routeCodec) <$> H.liftEffect getHash
    navigate $ fromMaybe Root initialRoute
  --  Handles the consecutive route changes.
  GoTo route e -> do
    liftEffect $ preventDefault (toEvent e)
    mRoute <- H.gets _.route
    when (mRoute /= Just route) $ navigate route

-- Renders a page component depending on which route is matched.
render :: forall m. MonadAff m => State -> H.ComponentHTML Action ChildSlots m
render st =
  skeleton (st.route)
    $ case st.route of
        Nothing -> HH.h1_ [ HH.text "Oh no! That page wasn't found!" ]
        Just route -> case route of
          Root -> HH.slot (Proxy :: _ "root") unit Root.component unit absurd
          Crystals -> HH.slot (Proxy :: _ "crystals") unit Crystals.component { sortColumn: Just "created", sortOrder: Descending } absurd
          P11Ingest -> HH.slot (Proxy :: _ "p11ingest") unit P11Ingest.component {} absurd
          Pucks -> HH.slot (Proxy :: _ "pucks") unit Pucks.component {} absurd
          Targets -> HH.slot (Proxy :: _ "targets") unit Targets.component {} absurd
          Jobs input -> HH.slot (Proxy :: _ "jobs") unit JobList.component input absurd
          CompoundOverview -> HH.slot (Proxy :: _ "compounds") unit Compounds.component unit absurd
          Job input -> HH.slot (Proxy :: _ "job") unit SingleJob.component input absurd
          ToolsAdmin -> HH.slot (Proxy :: _ "toolsadmin") unit ToolsAdmin.component unit absurd
          Analysis input -> HH.slot (Proxy :: _ "analysis") unit Analysis.component input absurd
          Tools input -> HH.slot (Proxy :: _ "tools") unit Tools.component input absurd
          Beamline input -> HH.slot (Proxy :: _ "beamline") unit Beamline.component input absurd

type NavItemData
  =
  { fa :: String
  , link :: Route
  , title :: String
  }

data NavItem
  = AtomicNavItem NavItemData
  | NestedNavItem { fa :: String, title :: String } (Array NavItemData)

navItems
  :: Array NavItem
navItems =
  [ NestedNavItem { fa: "vial", title: "Sample" }
      [ { title: "Compounds", link: CompoundOverview, fa: "atom" }
      , { title: "Crystals", link: Crystals, fa: "gem" }
      , { title: "Pucks", link: Pucks, fa: "hockey-puck" }
      , { title: "Targets", link: Targets, fa: "bullseye" }
      ]
  , AtomicNavItem
      { title: "Beamline"
      , link: Beamline { puckId: Nothing, crystalId: Nothing }
      , fa: "radiation"
      }
  , AtomicNavItem
      { title: "Analysis"
      , link:
          Analysis
            { sortOrder: Descending
            , sortColumn: Nothing
            , filterQuery: ""
            , selectedColumns: SelectedColumns []
            , limit: 500
            }
      , fa: "table"
      }
  , NestedNavItem { fa: "tools", title: "Processing" }
      [ { title: "Run tools", link: Tools {}, fa: "running" }
      , { title: "View jobs", link: Jobs { limit: 200, statusFilter: Nothing, tagFilter: Nothing, durationFilter: Nothing }, fa: "tasks" }
      , { title: "Administer tools", link: ToolsAdmin, fa: "screwdriver" }
      , { title: "P11 Ingest", link: P11Ingest, fa: "box-open" }
      ]
  ]

makeNavItem
  :: forall t163
   . Maybe Route
  -> NavItem
  -> HH.HTML t163 Action
makeNavItem route (NestedNavItem { fa, title } subNavItems) =
  let
    makeSubNav { fa: faSub, title: titleSub, link } =
      HH.a
        [ singleClass ("dropdown-item" <> (if fromMaybe false (sameRoute link <$> route) then " active" else ""))
        , HP.href (createLink link)
        , HE.onClick (GoTo link)
        ]
        [ icon { name: faSub, size: Nothing, spin: false }
        , HH.text (" " <> titleSub)
        ]

    active = case route of
      Nothing -> ""
      Just route' -> if any (\i -> sameRoute i.link route') subNavItems then " active" else ""
  in
    HH.li [ singleClass "nav-item dropdown" ]
      [ HH.a
          [ singleClass ("nav-link dropdown-toggle" <> active)
          , HP.attr (HH.AttrName "data-bs-toggle") "dropdown"
          , HP.href "#"
          ]
          [ icon { name: fa, size: Nothing, spin: false }, HH.text (" " <> title) ]
      , HH.div
          [ singleClass "dropdown-menu" ]
          (makeSubNav <$> subNavItems)
      ]

makeNavItem route (AtomicNavItem { title, link, fa }) =
  let
    isActive = if fromMaybe false (sameRoute link <$> route) then [ "active" ] else []
  in
    HH.li
      [ classList [ "nav-item" ] ]
      [ HH.a
          [ HP.href "#", HE.onClick (GoTo link), classList ([ "nav-link" ] <> isActive) ]
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
  HH.main [ HPA.role "main" ]
    [ html ]

skeleton :: forall a. Maybe Route -> HH.HTML a Action -> HH.HTML a Action
skeleton route html =
  -- In case you're wondering about this ID thing, it's to fix hot reloading, see also index.js and
  -- https://discourse.purescript.org/t/parcel-2-hot-reload-duplicates-page/2670/3
  HH.main [ HP.id "amarcord-web-app" ]
    [ HH.div
        [ classList [ "container" ] ]
        [ HH.header [ classList [ "d-flex", "flex-wrap", "justify-content-center", "py-3", "mb-4", "border-bottom" ] ]
            [ HH.img [ HP.src "desy-cfel.png", HP.alt "DESY and CFEL logo combined", classList [ "img-fluid", "amarcord-logo" ] ]
            , HH.a
                [ singleClass "d-flex align-items-center mb-3 mb-md-0 me-md-auto text-dark text-decoration-none" ]
                [ HH.span [ singleClass "fs-4" ] [ HH.text "AMARCORD" ] ]
            , HH.ul [ singleClass "nav nav-pills" ] (makeNavItem route <$> navItems)
            ]
        ]
    , HH.div [ singleClass "container-fluid" ]
        [ contentView html
        ]
    ]
