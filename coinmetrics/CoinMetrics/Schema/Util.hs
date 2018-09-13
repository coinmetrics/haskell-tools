{-# LANGUAGE GeneralizedNewtypeDeriving, LambdaCase, OverloadedStrings #-}

module CoinMetrics.Schema.Util
	( schemaJsonOptions
	, stripBeforeUnderscore
	, Base64ByteString(..)
	, Base16ByteString(..)
	) where

import Control.Monad
import qualified Data.Aeson as J
import qualified Data.ByteArray as BA
import qualified Data.ByteArray.Encoding as BA
import qualified Data.ByteString as B
import Data.Hashable
import qualified Data.Text.Encoding as T

import Hanalytics.Schema

schemaJsonOptions :: J.Options
schemaJsonOptions = J.defaultOptions
	{ J.fieldLabelModifier = stripBeforeUnderscore
	, J.constructorTagModifier = stripBeforeUnderscore
	}

-- | ByteString which serializes to JSON as base64 string.
newtype Base64ByteString = Base64ByteString B.ByteString deriving (Eq, Ord, Semigroup, Monoid, Hashable, BA.ByteArray, BA.ByteArrayAccess)
instance J.FromJSON Base64ByteString where
	parseJSON = either fail return . BA.convertFromBase BA.Base64URLUnpadded . T.encodeUtf8 <=< J.parseJSON
instance J.ToJSON Base64ByteString where
	toJSON = J.toJSON . T.decodeUtf8 . BA.convertToBase BA.Base64URLUnpadded

-- | ByteString which serializes to JSON as hex string.
newtype Base16ByteString = Base16ByteString B.ByteString deriving (Eq, Ord, Semigroup, Monoid, Hashable, BA.ByteArray, BA.ByteArrayAccess)
instance J.FromJSON Base16ByteString where
	parseJSON = either fail return . BA.convertFromBase BA.Base16 . T.encodeUtf8 <=< J.parseJSON
instance J.ToJSON Base16ByteString where
	toJSON = J.toJSON . T.decodeUtf8 . BA.convertToBase BA.Base16
