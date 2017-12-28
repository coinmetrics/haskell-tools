{-# LANGUAGE GeneralizedNewtypeDeriving, LambdaCase, OverloadedStrings #-}

module CoinMetrics.Schema.Util
	( schemaJsonOptions
	, stripBeforeUnderscore
	, Base64ByteString(..)
	, Base16ByteString(..)
	) where

import Control.Monad
import qualified Data.Aeson as J
import qualified Data.Avro.Schema as A
import qualified Data.ByteArray as BA
import qualified Data.ByteArray.Encoding as BA
import qualified Data.ByteString as B
import Data.Hashable
import qualified Data.Text as T
import qualified Data.Text.Encoding as T

schemaJsonOptions :: J.Options
schemaJsonOptions = J.defaultOptions
	{ J.fieldLabelModifier = stripBeforeUnderscore
	, J.constructorTagModifier = stripBeforeUnderscore
	}

stripBeforeUnderscore :: String -> String
stripBeforeUnderscore = \case
	(x : xs) -> case x of
		'_' -> xs
		_ -> stripBeforeUnderscore xs
	[] -> []

-- | ByteString which serializes to JSON as base64 string.
newtype Base64ByteString = Base64ByteString B.ByteString deriving (Eq, Ord, Monoid, Hashable, BA.ByteArray, BA.ByteArrayAccess)
instance J.FromJSON Base64ByteString where
	parseJSON = either fail return . BA.convertFromBase BA.Base64URLUnpadded . T.encodeUtf8 <=< J.parseJSON
instance J.ToJSON Base64ByteString where
	toJSON = J.toJSON . T.decodeUtf8 . BA.convertToBase BA.Base64URLUnpadded

-- | ByteString which serializes to JSON as hex string.
newtype Base16ByteString = Base16ByteString B.ByteString deriving (Eq, Ord, Monoid, Hashable, BA.ByteArray, BA.ByteArrayAccess)
instance J.FromJSON Base16ByteString where
	parseJSON = either fail return . BA.convertFromBase BA.Base16 . T.encodeUtf8 <=< J.parseJSON
instance J.ToJSON Base16ByteString where
	toJSON = J.toJSON . T.decodeUtf8 . BA.convertToBase BA.Base16
