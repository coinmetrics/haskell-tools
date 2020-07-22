{-# LANGUAGE DeriveGeneric, OverloadedStrings, PatternSynonyms, StandaloneDeriving, TemplateHaskell, TypeFamilies, ViewPatterns #-}
{-# OPTIONS_GHC -Wno-orphans #-}
{-# OPTIONS_GHC -Wno-missing-pattern-synonym-signatures #-}

module CoinMetrics.Stellar
  ( Stellar(..)
  , StellarLedger(..)
  , StellarTransaction(..)
  , StellarOperation(..)
  , StellarResult(..)
  ) where

import qualified Codec.Compression.GZip as GZip
import Control.Concurrent.STM
import Control.Exception
import Control.Monad
import qualified Crypto.Hash as C
import qualified Data.Aeson as J
import qualified Data.Aeson.Types as J
import Data.Bits
import qualified Data.ByteArray as BA
import qualified Data.ByteString as B
import qualified Data.ByteString.Lazy as BL
import qualified Data.ByteString.Short as BS
import qualified Data.ByteString.Unsafe as B
import qualified Data.Text.Encoding.Error as T
import Foreign.C.Types
import Foreign.Marshal.Alloc
import Foreign.Marshal.Array
import Foreign.Ptr
import Foreign.StablePtr
import Foreign.Storable
import qualified Data.HashMap.Strict as HM
import Data.Int
import Data.Maybe
import Data.Proxy
import Data.String
import qualified Data.Text as T
import qualified Data.Text.Encoding as T
import Data.Time.Clock.POSIX
import qualified Data.Vector as V
import qualified Network.HTTP.Client as H
import Numeric
import System.Environment
import System.IO.Unsafe(unsafeInterleaveIO, unsafePerformIO)

import CoinMetrics.BlockChain
import CoinMetrics.Schema.Util
import CoinMetrics.Util
import CoinMetrics.WebCache
import Hanalytics.Schema

-- | Stellar connector.
data Stellar = Stellar
  { stellar_webCache :: !WebCache
  , stellar_httpRequest :: !H.Request
  , stellar_checkpointCacheVar :: {-# UNPACK #-} !(TVar [(Int64, [StellarLedger])])
  , stellar_checkpointCacheSize :: {-# UNPACK #-} !Int
  }

data StellarLedger = StellarLedger
  { sl_sequence :: {-# UNPACK #-} !Int64
  , sl_hash :: {-# UNPACK #-} !HexString
  , sl_closeTime :: {-# UNPACK #-} !Int64
  , sl_raw :: !J.Value
  , sl_transactions :: !(V.Vector StellarTransaction)
  }

instance HasBlockHeader StellarLedger where
  getBlockHeader StellarLedger
    { sl_sequence = height
    , sl_hash = hash
    , sl_closeTime = closeTime
    } = BlockHeader
    { bh_height = height
    , bh_hash = hash
    , bh_prevHash = Nothing
    , bh_timestamp = posixSecondsToUTCTime $ fromIntegral closeTime
    }

data StellarTransaction = StellarTransaction
  { st_raw :: !J.Value
  , st_hash :: {-# UNPACK #-} !HexString
  , st_applyIndex :: {-# UNPACK #-} !Int64
  , st_rawResult :: !J.Value
  , st_operations :: !(V.Vector StellarOperation)
  }

data TransactionHistoryEntry = TransactionHistoryEntry
  { the_ledgerSeq :: {-# UNPACK #-} !Int64
  , the_transactions :: !(V.Vector StellarTransaction)
  }

data TransactionHistoryResultEntry = TransactionHistoryResultEntry
  { thre_ledgerSeq :: {-# UNPACK #-} !Int64
  , thre_results :: !(V.Vector (HexString, TransactionResult))
  }

newtype TransactionResult = TransactionResult J.Value

data StellarOperation = StellarOperation
  { so_raw :: !J.Value
  , so_result :: !StellarResult
  }

data StellarResult = StellarResult
  { sr_raw :: !J.Value
  }


genSchemaInstances [''StellarLedger, ''StellarTransaction, ''StellarOperation, ''StellarResult]


parseLedgers :: B.ByteString -> B.ByteString -> B.ByteString -> IO [StellarLedger]
parseLedgers ledgersBytes transactionsBytes resultsBytes = do
  ledgers <- parseSequence getLedgerHeaderHistoryEntry ledgersBytes
  transactions <- parseSequence getTransactionHistoryEntry transactionsBytes
  results <- parseSequence getTransactionHistoryResultEntry resultsBytes
  forM ledgers $ \ledger@StellarLedger
    { sl_sequence = ledgerSeq
    } -> do
    let
      ledgerTransactions = mconcat $ map the_transactions $ filter ((== ledgerSeq) . the_ledgerSeq) transactions
      ledgerResults = HM.fromList $ zipWith (\i (h, l) -> (h, (i, l))) [0..] $ V.toList $ mconcat $ map thre_results $ filter ((== ledgerSeq) . thre_ledgerSeq) results
    unless (V.length ledgerTransactions == HM.size ledgerResults) $ fail "transactions do not correspond to results"
    ledgerTransactionsWithResults <- forM ledgerTransactions $ \tx@(flip HM.lookup ledgerResults . st_hash -> Just (applyIndex, result)) -> combineTransactionAndResult tx
      { st_applyIndex = applyIndex
      } result
    return ledger
      { sl_transactions = ledgerTransactionsWithResults
      }
  where
    parseSequence :: (Ptr (Ptr CChar) -> Ptr CChar -> IO (Maybe a)) -> B.ByteString -> IO [a]
    parseSequence parse bytes = B.unsafeUseAsCStringLen bytes $ \(ptr, len) -> alloca $ \ptrPtr -> do
      poke ptrPtr ptr
      let
        endPtr = ptr `plusPtr` len
        step = do
          maybeObject <- parse ptrPtr endPtr
          case maybeObject of
            Just object -> do
              (object :) <$> step
            Nothing -> return []
        in step

    getLedgerHeaderHistoryEntry :: Ptr (Ptr CChar) -> Ptr CChar -> IO (Maybe StellarLedger)
    getLedgerHeaderHistoryEntry ptrPtr len = traverse f =<< decodeObject c_decodeLedgerHistoryEntry ptrPtr len where
      f = either fail return . J.parseEither g
      g = J.withObject "LedgerHeaderHistoryEntry" $ \v -> do
        header <- v J..: "header"
        scpValue <- header J..: "scpValue"
        StellarLedger
          <$> header J..: "ledgerSeq"
          <*> v J..: "hash"
          <*> scpValue J..: "closeTime"
          <*> pure (J.Object v)
          <*> pure V.empty

    getTransactionHistoryEntry :: Ptr (Ptr CChar) -> Ptr CChar -> IO (Maybe TransactionHistoryEntry)
    getTransactionHistoryEntry = decodeObject decode where
      decode ptr len = B.unsafeUseAsCString productionNetworkId $ \networkIdPtr -> c_decodeTransactionHistoryEntry ptr len networkIdPtr

    getTransactionHistoryResultEntry :: Ptr (Ptr CChar) -> Ptr CChar -> IO (Maybe TransactionHistoryResultEntry)
    getTransactionHistoryResultEntry ptrPtr len = traverse f =<< decodeObject c_decodeTransactionHistoryResultEntry ptrPtr len where
      f = either fail return . J.parseEither g
      g = J.withObject "TransactionHistoryResultEntry" $ \v -> do
        ledgerSeq <- v J..: "ledgerSeq"
        txResultSet <- v J..: "txResultSet"
        resultPairs <- txResultSet J..: "results"
        results <- V.forM resultPairs $ \resultPair -> do
          hash <- resultPair J..: "transactionHash"
          result <- resultPair J..: "result"
          return (hash, TransactionResult result)
        return TransactionHistoryResultEntry
          { thre_ledgerSeq = ledgerSeq
          , thre_results = results
          }

    decodeObject :: (Ptr (Ptr CChar) -> Ptr CChar -> IO (StablePtr a)) -> Ptr (Ptr CChar) -> Ptr CChar -> IO (Maybe a)
    decodeObject f ptrPtr len = do
      sp <- f ptrPtr len
      if castStablePtrToPtr sp == nullPtr
        then return Nothing
        else do
          p <- deRefStablePtr sp
          freeStablePtr sp
          return $ Just p

    combineTransactionAndResult :: StellarTransaction -> TransactionResult -> IO StellarTransaction
    combineTransactionAndResult transaction@StellarTransaction
      { st_operations = operations
      } (TransactionResult resultValue) = do
      opResults <- either fail (return . fromMaybe V.empty) . flip J.parseEither resultValue $ J.withObject "TransactionResult" $ \obj -> do
        result <- obj J..: "result"
        result J..:? "results"
      -- some results are missing, allow that
      unless (V.length operations == V.length opResults || V.length opResults == 0) $ fail "operations do not correspond to results"
      return transaction
        { st_rawResult = resultValue
        , st_operations = V.zipWith (\operation result -> operation
          { so_result = StellarResult result
          }) operations opResults
        }


combineTransactionHistoryEntryWithHashes :: J.Value -> V.Vector HexString -> IO TransactionHistoryEntry
combineTransactionHistoryEntryWithHashes value hashes = either fail return $ J.parseEither f value where
  f = J.withObject "TransactionHistoryEntry" $ \v -> do
    ledgerSeq <- v J..: "ledgerSeq"
    txSet <- v J..: "txSet"
    txs <- txSet J..: "txs"
    unless (V.length txs == V.length hashes) $ fail "wrong number of transaction hashes"
    transactions <- V.zipWithM t txs hashes
    return TransactionHistoryEntry
      { the_ledgerSeq = ledgerSeq
      , the_transactions = transactions
      }
  t transaction txHash = do
    txType <- transaction J..: "type"
    tx <- case txType :: T.Text of
      "ENVELOPE_TYPE_TX_V0" -> transaction J..: "v0"
      "ENVELOPE_TYPE_TX" -> transaction J..: "v1"
      "ENVELOPE_TYPE_TX_FEE_BUMP" -> transaction J..: "feeBump"
      _ -> fail "unknown transaction type"
    operations <- (fmap $ fromMaybe V.empty) . (J..:? "operations") =<< tx J..: "tx"
    return StellarTransaction
      { st_raw = J.Object transaction
      , st_hash = txHash
      , st_applyIndex = -1
      , st_rawResult = J.Null -- will be combined later
      , st_operations = V.map o operations
      }
  o operation = StellarOperation
    { so_raw = operation
    , so_result = StellarResult J.Null -- will be retrieved later
    }


stellarRequest :: Stellar -> Bool -> T.Text -> IO BL.ByteString
stellarRequest Stellar
  { stellar_webCache = webCache
  , stellar_httpRequest = httpRequest
  } skipCache path = either fail return <=< tryWithRepeat $ requestWebCache webCache skipCache httpRequest
  { H.path = (if H.path httpRequest == "/" then "" else H.path httpRequest) <> T.encodeUtf8 path
  } $ \body -> return (True, body)

withCheckpointCache :: Stellar -> Int64 -> IO [StellarLedger]
withCheckpointCache stellar@Stellar
  { stellar_checkpointCacheVar = cacheVar
  , stellar_checkpointCacheSize = cacheSize
  } checkpointSequence = do
  lazyLedgers <- unsafeInterleaveIO io
  atomically $ do
    cache <- readTVar cacheVar
    case filter ((== checkpointSequence) . fst) cache of
      (_, cachedLedgers) : _ -> return cachedLedgers
      [] -> do
        writeTVar cacheVar $!
          (if length cache >= cacheSize then init cache else cache)
          ++ [(checkpointSequence, lazyLedgers)]
        return lazyLedgers
  where
    io = do
      ledgersBytes <- GZip.decompress <$> stellarRequest stellar False ledgerPath
      transactionsBytes <- GZip.decompress <$> stellarRequest stellar False transactionsPath
      resultsBytes <- GZip.decompress <$> stellarRequest stellar False resultsPath
      parseLedgers (BL.toStrict ledgersBytes) (BL.toStrict transactionsBytes) (BL.toStrict resultsBytes)
    sequenceHex = showHex checkpointSequence ""
    sequenceHexPadded@[h0, h1, h2, h3, h4, h5, _h6, _h7]
      = replicate (max 0 (8 - length sequenceHex)) '0' ++ sequenceHex
    ledgerPath = "/ledger/" <> T.pack [h0, h1, '/', h2, h3, '/', h4, h5] <> "/ledger-" <> T.pack sequenceHexPadded <> ".xdr.gz"
    transactionsPath = "/transactions/" <> T.pack [h0, h1, '/', h2, h3, '/', h4, h5] <> "/transactions-" <> T.pack sequenceHexPadded <> ".xdr.gz"
    resultsPath = "/results/" <> T.pack [h0, h1, '/', h2, h3, '/', h4, h5] <> "/results-" <> T.pack sequenceHexPadded <> ".xdr.gz"

instance BlockChain Stellar where
  type Block Stellar = StellarLedger

  getBlockChainInfo _ = BlockChainInfo
    { bci_init = \BlockChainParams
      { bcp_httpManager = httpManager
      , bcp_httpRequest = httpRequest
      , bcp_threadsCount = threadsCount
      } -> do
      webCache <- initWebCache httpManager
      checkpointCacheVar <- newTVarIO []
      return Stellar
        { stellar_webCache = webCache
        , stellar_httpRequest = httpRequest
        , stellar_checkpointCacheVar = checkpointCacheVar
        , stellar_checkpointCacheSize = 2 + threadsCount `quot` 64
        }
    , bci_defaultApiUrls =
      [ "https://history.stellar.org/prd/core-live/core_live_001"
      , "https://history.stellar.org/prd/core-live/core_live_002"
      , "https://history.stellar.org/prd/core-live/core_live_003"
      ]
    , bci_defaultBeginBlock = 1
    , bci_defaultEndBlock = 0 -- history data, no rewrites
    , bci_heightFieldName = "sequence"
    , bci_schemas = standardBlockChainSchemas
      (schemaOf (Proxy :: Proxy StellarLedger))
      [ schemaOf (Proxy :: Proxy StellarResult)
      , schemaOf (Proxy :: Proxy StellarOperation)
      , schemaOf (Proxy :: Proxy StellarTransaction)
      ]
      "CREATE TABLE \"stellar\" OF \"StellarLedger\" (PRIMARY KEY (\"sequence\"));"
    }

  getCurrentBlockHeight stellar = either fail return
    . (J.parseEither (J..: "currentLedger") <=< J.eitherDecode)
    =<< stellarRequest stellar True "/.well-known/stellar-history.json"

  getBlockByHeight stellar blockHeight = do
    ledgers <- filter ((== blockHeight) . sl_sequence) <$>
      withCheckpointCache stellar (((blockHeight + 64) .&. complement 0x3f) - 1)
    case ledgers of
      [ledger] -> return ledger
      _ -> fail "ledger cache error"


-- | Network id. Defines how to calculate hashes of transactions.
{-# NOINLINE productionNetworkId #-}
productionNetworkId :: B.ByteString
productionNetworkId = unsafePerformIO $ hash . fromString . fromMaybe "Public Global Stellar Network ; September 2015" <$> lookupEnv "STELLAR_NETWORK_ID"
  where
    hash networkId = BA.convert (C.hash (networkId :: B.ByteString) :: C.Digest C.SHA256)

export_encodeJson :: Ptr CChar -> CSize -> IO (StablePtr J.Value)
export_encodeJson ptr len =
  either fail newStablePtr . J.eitherDecode' . BL.fromStrict
  . T.encodeUtf8 . T.decodeUtf8With T.lenientDecode
  =<< B.unsafePackCStringLen (ptr, fromIntegral len)

export_hash :: Ptr CChar -> CSize -> IO (StablePtr HexString)
export_hash ptr len = do
  bytes <- B.unsafePackCStringLen (ptr, fromIntegral len)
  hash <- evaluate $ HexString $ BS.toShort $ BA.convert (C.hash bytes :: C.Digest C.SHA256)
  newStablePtr hash

export_combineTransactionHistoryEntryWithHashes :: StablePtr J.Value -> Ptr (StablePtr HexString) -> CSize -> IO (StablePtr TransactionHistoryEntry)
export_combineTransactionHistoryEntryWithHashes valueSPtr hashesSPtrsPtr hashesCount = do
  value <- deRefStablePtr valueSPtr
  freeStablePtr valueSPtr
  hashesSPtrs <- V.fromList <$> peekArray (fromIntegral hashesCount) hashesSPtrsPtr
  hashes <- V.forM hashesSPtrs $ \hashSPtr -> do
    hash <- deRefStablePtr hashSPtr
    freeStablePtr hashSPtr
    return hash
  newStablePtr =<< combineTransactionHistoryEntryWithHashes value hashes

-- imported functions
foreign import ccall safe "coinmetrics_stellar_decode_ledger_history_entry" c_decodeLedgerHistoryEntry :: Ptr (Ptr CChar) -> Ptr CChar -> IO (StablePtr J.Value)
foreign import ccall safe "coinmetrics_stellar_decode_transaction_history_entry" c_decodeTransactionHistoryEntry :: Ptr (Ptr CChar) -> Ptr CChar -> Ptr CChar -> IO (StablePtr TransactionHistoryEntry)
foreign import ccall safe "coinmetrics_stellar_decode_result" c_decodeTransactionHistoryResultEntry :: Ptr (Ptr CChar) -> Ptr CChar -> IO (StablePtr J.Value)
-- exported functions
foreign export ccall "coinmetrics_stellar_encode_json" export_encodeJson :: Ptr CChar -> CSize -> IO (StablePtr J.Value)
foreign export ccall "coinmetrics_stellar_hash" export_hash :: Ptr CChar -> CSize -> IO (StablePtr HexString)
foreign export ccall "coinmetrics_stellar_combine_transaction_history_entry_with_hashes" export_combineTransactionHistoryEntryWithHashes :: StablePtr J.Value -> Ptr (StablePtr HexString) -> CSize -> IO (StablePtr TransactionHistoryEntry)
