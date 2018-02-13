{-# LANGUAGE OverloadedLists, OverloadedStrings, ViewPatterns #-}

module CoinMetrics.JsonRpc
	( JsonRpc()
	, newJsonRpc
	, jsonRpcRequest
	) where

import qualified Data.Aeson as J
import qualified Data.Aeson.Types as J
import qualified Data.Text as T
import qualified Data.Text.Encoding as T
import qualified Data.Vector as V
import qualified Network.HTTP.Client as H

import CoinMetrics.Util

data JsonRpc = JsonRpc
	{ jsonRpc_httpManager :: !H.Manager
	, jsonRpc_httpRequest :: !H.Request
	}

newJsonRpc :: H.Manager -> H.Request -> Maybe (T.Text, T.Text) -> JsonRpc
newJsonRpc httpManager httpRequest maybeCredentials = JsonRpc
	{ jsonRpc_httpManager = httpManager
	, jsonRpc_httpRequest = (maybe id (\(authName, authPass) -> H.applyBasicAuth (T.encodeUtf8 authName) (T.encodeUtf8 authPass)) maybeCredentials) httpRequest
		{ H.method = "POST"
		, H.requestHeaders = [("Content-Type", "application/json")]
		}
	}

jsonRpcRequest :: J.FromJSON r => JsonRpc -> T.Text -> V.Vector J.Value -> IO r
jsonRpcRequest JsonRpc
	{ jsonRpc_httpManager = httpManager
	, jsonRpc_httpRequest = httpRequest
	} method params = do
	body <- H.responseBody <$> tryWithRepeat (H.httpLbs httpRequest
		{ H.requestBody = H.RequestBodyLBS $ J.encode $ J.Object
			[ ("jsonrpc", "2.0")
			, ("method", J.String method)
			, ("params", J.Array params)
			, ("id", J.String "1")
			]
		} httpManager)
	case J.eitherDecode' body of
		Right obj -> case J.parse (J..: "result") obj of
			J.Success result -> return result
			J.Error err -> fail err
		Left err -> fail err
