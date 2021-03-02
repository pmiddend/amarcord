module App.API where

import Prelude

import App.AppMonad (AppMonad)
import Control.Monad.Reader (asks)
import Data.Either (Either)
import GraphQLClient (GraphQLError, Scope__RootMutation, Scope__RootQuery, SelectionSet, defaultRequestOptions, graphqlMutationRequest, graphqlQueryRequest)
import Halogen (liftAff)

graphqlQuery :: forall t96. SelectionSet Scope__RootQuery t96 -> AppMonad (Either (GraphQLError t96) t96)
graphqlQuery ss = do
  baseUrl' <- asks (_.baseUrl)
  liftAff $ graphqlQueryRequest baseUrl' defaultRequestOptions ss

graphqlMutation :: forall t98. SelectionSet Scope__RootMutation t98 -> AppMonad (Either (GraphQLError t98) t98)
graphqlMutation ss = do
  baseUrl' <- asks (_.baseUrl)
  liftAff $ graphqlMutationRequest baseUrl' defaultRequestOptions ss

