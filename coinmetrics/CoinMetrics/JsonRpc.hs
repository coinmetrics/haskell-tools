{-# LANGUAGE OverloadedStrings, ViewPatterns #-}

module CoinMetrics.JsonRpc
	( JsonRpc()
	, newJsonRpc
	, jsonRpcRequest
	) where

import Control.Concurrent
import Control.Exception
import qualified Data.Aeson as J
import qualified Data.HashMap.Strict as HM
import qualified Data.Text as T
import qualified Data.Text.Encoding as T
import qualified Data.Vector as V
import qualified Network.HTTP.Client as H

data JsonRpc = JsonRpc
	{ jsonRpc_httpManager :: !H.Manager
	, jsonRpc_httpRequest :: !H.Request
	}

newJsonRpc :: H.Manager -> T.Text -> Int -> Maybe (T.Text, T.Text) -> JsonRpc
newJsonRpc httpManager host port maybeCredentials = JsonRpc
	{ jsonRpc_httpManager = httpManager
	, jsonRpc_httpRequest = (maybe id (\(authName, authPass) -> H.applyBasicAuth (T.encodeUtf8 authName) (T.encodeUtf8 authPass)) maybeCredentials) H.defaultRequest
		{ H.method = "POST"
		, H.secure = False
		, H.host = T.encodeUtf8 host
		, H.port = port
		, H.requestHeaders = [("Content-Type", "application/json")]
		}
	}

jsonRpcRequest :: JsonRpc -> T.Text -> V.Vector J.Value -> IO J.Value
jsonRpcRequest JsonRpc
	{ jsonRpc_httpManager = httpManager
	, jsonRpc_httpRequest = httpRequest
	} method params = do
	body <- H.responseBody <$> tryWithRepeat (H.httpLbs httpRequest
		{ H.requestBody = H.RequestBodyLBS $ J.encode $ J.Object $ HM.fromList
			[ ("jsonrpc", "2.0")
			, ("method", J.String method)
			, ("params", J.Array params)
			, ("id", J.String "1")
			]
		} httpManager)
	case J.eitherDecode' body of
		Right (J.Object (HM.lookup "result" -> Just resultValue)) -> return resultValue
		Left err -> fail err
		_ -> fail "bad json rpc response"

tryWithRepeat :: IO a -> IO a
tryWithRepeat io = let
	step = do
		eitherResult <- try io
		case eitherResult of
			Right result -> return result
			Left (SomeException err) -> do
				putStrLn $ "error: " ++ show err ++ ", retrying again in 10 seconds"
				threadDelay 10000000
				step
	in step
