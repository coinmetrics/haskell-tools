{-# LANGUAGE DeriveGeneric, LambdaCase, OverloadedLists, OverloadedStrings, StandaloneDeriving, TemplateHaskell, TypeFamilies, ViewPatterns #-}

module CoinMetrics.Ripple
  ( Ripple(..)
  , RippleLedger(..)
  , RippleTransaction(..)
  ) where

import Control.Exception
import Control.Monad
import qualified Data.Aeson as J
import qualified Data.Aeson.Types as J
import qualified Data.ByteString as B
import qualified Data.ByteString.Lazy as BL
import qualified Data.HashMap.Strict as HM
import Data.Int
import Data.Maybe
import Data.Proxy
import Data.Scientific
import qualified Data.Text as T
import qualified Data.Text.Encoding as T
import qualified Data.Time.Clock.POSIX as Time
import qualified Data.Time.ISO8601 as Time
import qualified Data.Vector as V
import qualified Network.HTTP.Client as H
import System.IO
import Text.ParserCombinators.ReadP

import CoinMetrics.BlockChain
import CoinMetrics.JsonRpc
import CoinMetrics.Schema.Util
-- import CoinMetrics.Schema.Flatten
import CoinMetrics.Util
import CoinMetrics.WebCache
import Hanalytics.Schema

-- | Ripple connector.
data Ripple
  -- | Ripple Data API.
  = RippleDataApi
    { ripple_webCache :: !WebCache
    , ripple_httpRequest :: !H.Request
    }
  -- | Ripple JSON RPC API.
  | RippleJsonRpcApi
    { ripple_jsonRpc :: !JsonRpc
    }

rippleRequest :: J.FromJSON r => Ripple -> Bool -> T.Text -> [(B.ByteString, Maybe B.ByteString)] -> T.Text -> J.Object -> IO r
rippleRequest ripple skipCache dataPath dataParams rpcMethod rpcParams = case ripple of
  RippleDataApi
    { ripple_webCache = webCache
    , ripple_httpRequest = httpRequest
    } -> either fail return <=< tryWithRepeat $ do
    requestWebCache webCache skipCache (H.setQueryString dataParams httpRequest
      { H.path = T.encodeUtf8 dataPath
      }) $ \case
        -- response
        Right body -> case J.eitherDecode body of
          -- correctly decoded response
          Right decodedBody -> return (True, Right decodedBody)
          -- temporary failure
          Left err -> do
            hPutStrLn stderr $ "wrong ripple response for " <> T.unpack dataPath <> ": " <> T.unpack (T.decodeUtf8 $ BL.toStrict $ BL.take 256 body)
            fail err
        -- persistent failure
        Left err -> return (False, Left err)
  RippleJsonRpcApi
    { ripple_jsonRpc = jsonRpc
    } -> jsonRpcRequest jsonRpc rpcMethod (J.Array [J.Object rpcParams])

-- https://ripple.com/build/data-api-v2/#ledger-objects

data RippleLedger = RippleLedger
  { rl_index :: {-# UNPACK #-} !Int64
  , rl_hash :: {-# UNPACK #-} !HexString
  , rl_totalCoins :: {-# UNPACK #-} !Scientific
  , rl_closeTime :: {-# UNPACK #-} !Int64
  , rl_transactions :: !(V.Vector (Maybe RippleTransaction))
  }

instance HasBlockHeader RippleLedger where
  getBlockHeader RippleLedger
    { rl_index = index
    , rl_hash = hash
    , rl_closeTime = closeTime
    } = BlockHeader
    { bh_height = index
    , bh_hash = hash
    , bh_prevHash = Nothing
    , bh_timestamp = Time.posixSecondsToUTCTime $ fromIntegral closeTime
    }

newtype RippleLedgerWrapper = RippleLedgerWrapper
  { unwrapRippleLedger :: RippleLedger
  }

instance J.FromJSON RippleLedgerWrapper where
  parseJSON = J.withObject "ripple ledger" $ \fields -> fmap RippleLedgerWrapper $ RippleLedger
    <$> (decodeMaybeFromText =<< fields J..: "ledger_index")
    <*> (fields J..: "ledger_hash")
    <*> (decodeAmount =<< fields J..: "total_coins")
    <*> (fields J..: "close_time")
    <*> (V.map (unwrapRippleTransaction <$>) <$> fields J..: "transactions")

data RippleTransaction = RippleTransaction
  { rt_hash :: {-# UNPACK #-} !HexString
  , rt_date :: !(Maybe Int64)
  , rt_account :: !T.Text
  , rt_fee :: {-# UNPACK #-} !Scientific
  , rt_sequence :: {-# UNPACK #-} !Int64
  , rt_accountTxnId :: !(Maybe HexString)
  , rt_flags :: {-# UNPACK #-} !Int64
  , rt_lastLedgerSequence :: !(Maybe Int64)
  , rt_type :: !T.Text
  , rt_amount :: !(Maybe RippleCurrencyAmount)
  , rt_balance :: !(Maybe RippleCurrencyAmount)
  , rt_channel :: !(Maybe T.Text)
  , rt_destination :: !(Maybe T.Text)
  , rt_result :: !T.Text
  , rt_deliveredAmount :: !(Maybe RippleCurrencyAmount)
  , rt_raw :: !J.Value
  }

newtype RippleTransactionWrapper = RippleTransactionWrapper
  { unwrapRippleTransaction :: RippleTransaction
  }

instance J.FromJSON RippleTransactionWrapper where
  parseJSON = J.withObject "ripple transaction" $ \fields -> do
    tx <- fromMaybe fields <$> fields J..:? "tx"
    meta <- maybe (fields J..: "metaData") return =<< fields J..:? "meta"
    fmap RippleTransactionWrapper $ RippleTransaction
      <$> (fields J..: "hash")
      <*> (traverse decodeDate =<< fields J..:? "date")
      <*> (tx J..: "Account")
      <*> (decodeAmount =<< tx J..: "Fee")
      <*> (tx J..: "Sequence")
      <*> (tx J..:? "AccountTxnID")
      <*> (fromMaybe 0 <$> tx J..:? "Flags")
      <*> (tx J..:? "LastLedgerSequence")
      <*> (tx J..: "TransactionType")
      <*> (traverse decodeCurrencyAmount =<< tx J..:? "Amount")
      <*> (traverse decodeCurrencyAmount =<< tx J..:? "Balance")
      <*> (tx J..:? "Channel")
      <*> (tx J..:? "Destination")
      <*> (meta J..: "TransactionResult")
      <*> (maybe (return Nothing) decodeDeliveredAmount =<< maybe (meta J..:? "DeliveredAmount") (return . Just) =<< meta J..:? "delivered_amount")
      <*> (return $ J.Object fields)

data RippleCurrencyAmount = RippleCurrencyAmount
  { rca_amount :: {-# UNPACK #-} !Scientific
  , rca_currency :: !(Maybe T.Text)
  , rca_issuer :: !(Maybe T.Text)
  }

decodeCurrencyAmount :: J.Value -> J.Parser RippleCurrencyAmount
decodeCurrencyAmount = \case
  J.String amountStr -> RippleCurrencyAmount
    <$> (decodeAmount amountStr)
    <*> (return Nothing)
    <*> (return Nothing)
  J.Object amountObject -> RippleCurrencyAmount
    <$> (decodeAmount =<< amountObject J..: "value")
    <*> (amountObject J..: "currency")
    <*> (amountObject J..: "issuer")
  _ -> fail "wrong currency amount"

decodeDeliveredAmount :: J.Value -> J.Parser (Maybe RippleCurrencyAmount)
decodeDeliveredAmount = \case
  J.String "unavailable" -> return Nothing
  value -> Just <$> decodeCurrencyAmount value

decodeAmount :: T.Text -> J.Parser Scientific
decodeAmount (T.unpack -> t) = case readP_to_S scientificP t of
  [(value, "")] -> return value
  _ -> fail $ "wrong amount: " <> t

decodeDate :: T.Text -> J.Parser Int64
decodeDate (T.unpack -> t) = case Time.parseISO8601 t of
  Just date -> return $ floor $ Time.utcTimeToPOSIXSeconds date
  Nothing -> fail $ "wrong date: " <> t


genSchemaInstances [''RippleLedger, ''RippleTransaction, ''RippleCurrencyAmount]
-- doesn't work because of Vector (Maybe RippleTransaction)
-- genFlattenedTypes "index" [| rl_index |] [("ledger", ''RippleLedger), ("transaction", ''RippleTransaction)]

instance BlockChain Ripple where
  type Block Ripple = RippleLedger

  getBlockChainInfo _ = BlockChainInfo
    { bci_init = \BlockChainParams
      { bcp_httpManager = httpManager
      , bcp_httpRequest = httpRequest
      } -> do
      let jsonRpc = newJsonRpc httpManager httpRequest Nothing
      r <- try $ jsonRpcRequest jsonRpc "server_info" (J.Array [])
      case r :: Either SomeException J.Object of
        Right (J.parseEither (J..: "status") -> Right (J.String "success")) -> return $ RippleJsonRpcApi jsonRpc
        _ -> do
          webCache <- initWebCache httpManager
          return RippleDataApi
            { ripple_webCache = webCache
            , ripple_httpRequest = httpRequest
            }
    , bci_defaultApiUrls = ["https://data.ripple.com/"]
    , bci_defaultBeginBlock = 32570 -- genesis ledger
    , bci_defaultEndBlock = 0 -- history data, no rewrites
    , bci_heightFieldName = "index"
    , bci_schemas = standardBlockChainSchemas
      (schemaOf (Proxy :: Proxy RippleLedger))
      [ schemaOf (Proxy :: Proxy RippleCurrencyAmount)
      , schemaOf (Proxy :: Proxy RippleTransaction)
      ]
      "CREATE TABLE \"ripple\" OF \"RippleLedger\" (PRIMARY KEY (\"index\"));"
    -- , bci_flattenSuffixes = ["ledgers", "transactions"]
    -- , bci_flattenPack = let
    --  f (ledgers, transactions) =
    --    [ SomeBlocks (ledgers :: [RippleLedger_flattened])
    --    , SomeBlocks (transactions :: [RippleTransaction_flattened])
    --    ]
    --  in f . mconcat . map flatten
    }

  getCurrentBlockHeight ripple = either fail return
    . J.parseEither (decodeMaybeFromText <=< (J..: "ledger_index") <=< (J..: "ledger"))
    =<< rippleRequest ripple True "/v2/ledgers" [] "ledger" [("ledger_index", J.String "validated")]

  getBlockByHeight ripple blockHeight = do
    eitherLedger <- J.parseEither (J..: "ledger")
      <$> rippleRequest ripple False
        -- Data API path and params
        ("/v2/ledgers/" <> T.pack (show blockHeight))
        [ ("transactions", Just "true")
        , ("expand", Just "true")
        ]
        -- JSON RPC API path and params
        "ledger"
        [ ("ledger_index", J.toJSON blockHeight)
        , ("transactions", J.toJSON True)
        , ("expand", J.toJSON True)
        ]
    case eitherLedger of
      Right ledger -> return $ unwrapRippleLedgerWithCorrection ripple ledger
      Left _e -> do
        -- fallback to retrieving transactions individually
        preLedger <- either fail return
          . J.parseEither (J..: "ledger")
          =<< rippleRequest ripple False
            -- Data API path and params
            ("/v2/ledgers/" <> T.pack (show blockHeight))
            [ ("transactions", Just "true")
            ]
            -- JSON RPC API path and params
            "ledger"
            [ ("ledger_index", J.toJSON blockHeight)
            , ("transactions", J.toJSON True)
            ]
        transactionsHashes <- either fail return $ J.parseEither (J..: "transactions") preLedger
        transactions <- forM transactionsHashes $ \transactionHash ->
          either (const J.Null) J.Object . J.parseEither (J..: "transaction")
            <$> rippleRequest ripple False
              -- Data API path and params
              ("/v2/transactions/" <> transactionHash) []
              -- JSON RPC API path and params
              "tx" [("transaction", J.toJSON transactionHash)]
        either fail (return . unwrapRippleLedgerWithCorrection ripple) $ J.parseEither J.parseJSON $ J.Object $ HM.insert "transactions" (J.Array transactions) preLedger

unwrapRippleLedgerWithCorrection :: Ripple -> RippleLedgerWrapper -> RippleLedger
unwrapRippleLedgerWithCorrection ripple RippleLedgerWrapper
  { unwrapRippleLedger = ledger
  } = case ripple of
  RippleDataApi {} -> ledger
  RippleJsonRpcApi {} -> ledger
    { rl_closeTime = rl_closeTime ledger + 946684800 -- https://developers.ripple.com/basic-data-types.html#specifying-time
    }
