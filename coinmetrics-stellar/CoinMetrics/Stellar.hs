{-# LANGUAGE DeriveGeneric, OverloadedStrings, PatternSynonyms, StandaloneDeriving, TemplateHaskell, TypeFamilies, ViewPatterns #-}
{-# OPTIONS_GHC -Wno-orphans #-}
{-# OPTIONS_GHC -Wno-missing-pattern-synonym-signatures #-}

module CoinMetrics.Stellar
  ( Stellar(..)
  , StellarLedger(..)
  , StellarTransaction(..)
  , StellarOperation(..)
  , StellarAsset(..)
  ) where

import qualified Codec.Compression.GZip as GZip
import Control.Concurrent.STM
import Control.Monad
import qualified Crypto.Hash as C
import qualified Data.Aeson as J
import qualified Data.Aeson.Types as J
import Data.Bits
import qualified Data.ByteArray as BA
import qualified Data.ByteString as B
import qualified Data.ByteString.Lazy as BL
import qualified Data.ByteString.Short as BS
import Data.Default
import qualified Data.HashMap.Strict as HM
import Data.Int
import Data.Maybe
import Data.Proxy
import qualified Data.Serialize as S
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
  , sl_totalCoins :: {-# UNPACK #-} !Int64
  , sl_feePool :: {-# UNPACK #-} !Int64
  , sl_inflationSeq :: {-# UNPACK #-} !Int64
  , sl_idPool :: {-# UNPACK #-} !Int64
  , sl_baseFee :: {-# UNPACK #-} !Int64
  , sl_baseReserve :: {-# UNPACK #-} !Int64
  , sl_maxTxSetSize :: {-# UNPACK #-} !Int64
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
  { st_hash :: {-# UNPACK #-} !HexString
  , st_applyIndex :: {-# UNPACK #-} !Int64
  , st_sourceAccount :: {-# UNPACK #-} !HexString
  , st_fee :: {-# UNPACK #-} !Int64
  , st_seqNum :: {-# UNPACK #-} !Int64
  , st_timeBoundsMinTime :: !(Maybe Int64)
  , st_timeBoundsMaxTime :: !(Maybe Int64)
  , st_feeCharged :: {-# UNPACK #-} !Int64
  , st_resultCode :: {-# UNPACK #-} !Int64
  , st_operations :: !(V.Vector StellarOperation)
  }

data StellarOperation = StellarOperation
  { so_type :: {-# UNPACK #-} !Int64
  , so_sourceAccount :: !(Maybe HexString)
  , so_amount :: !(Maybe Int64)
  , so_asset :: !(Maybe StellarAsset)
  , so_assetCode :: !(Maybe T.Text)
  , so_authorize :: !(Maybe Bool)
  , so_bumpTo :: !(Maybe Int64)
  , so_buying :: !(Maybe StellarAsset)
  , so_clearFlags :: !(Maybe Int64)
  , so_dataName :: !(Maybe T.Text)
  , so_dataValue :: !(Maybe HexString)
  , so_destAmount :: !(Maybe Int64)
  , so_destAsset :: !(Maybe StellarAsset)
  , so_destination :: !(Maybe HexString)
  , so_highThreshold :: !(Maybe Int64)
  , so_homeDomain :: !(Maybe T.Text)
  , so_inflationDest :: !(Maybe HexString)
  , so_limit :: !(Maybe Int64)
  , so_line :: !(Maybe StellarAsset)
  , so_lowThreshold :: !(Maybe Int64)
  , so_masterWeight :: !(Maybe Int64)
  , so_medThreshold :: !(Maybe Int64)
  , so_offerID :: !(Maybe Int64)
  , so_path :: !(V.Vector StellarAsset)
  , so_price :: !(Maybe Double)
  , so_selling :: !(Maybe StellarAsset)
  , so_sendAsset :: !(Maybe StellarAsset)
  , so_sendMax :: !(Maybe Int64)
  , so_setFlags :: !(Maybe Int64)
  , so_startingBalance :: !(Maybe Int64)
  , so_trustor :: !(Maybe HexString)
  , so_resultCode :: !(Maybe Int64)
  , so_result :: !(Maybe Int64)
  , so_resultDestination :: !(Maybe HexString)
  , so_resultAsset :: !(Maybe StellarAsset)
  , so_resultAmount :: !(Maybe Int64)
  , so_resultEffect :: !(Maybe Int64)
  , so_resultClaimOffers :: !(V.Vector StellarClaimOfferAtom)
  , so_resultOffer :: !(Maybe StellarOfferEntry)
  , so_resultInflationPayouts :: !(V.Vector StellarInflationPayout)
  }

data StellarClaimOfferAtom = StellarClaimOfferAtom
  { scoa_sellerID :: {-# UNPACK #-} !HexString
  , scoa_offerID :: {-# UNPACK #-} !Int64
  , scoa_assetSold :: !StellarAsset
  , scoa_amountSold :: {-# UNPACK #-} !Int64
  , scoa_assetBought :: !StellarAsset
  , scoa_amountBought :: {-# UNPACK #-} !Int64
  }

data StellarOfferEntry = StellarOfferEntry
  { soe_sellerID :: {-# UNPACK #-} !HexString
  , soe_offerID :: {-# UNPACK #-} !Int64
  , soe_selling :: !StellarAsset
  , soe_buying :: !StellarAsset
  , soe_amount :: {-# UNPACK #-} !Int64
  , soe_price :: {-# UNPACK #-} !Double
  , soe_flags :: {-# UNPACK #-} !Int64
  }

data StellarInflationPayout = StellarInflationPayout
  { sip_destination :: {-# UNPACK #-} !HexString
  , sip_amount :: !Int64
  }

instance Default StellarOperation

pattern SOT_CREATE_ACCOUNT = 0
pattern SOT_PAYMENT = 1
pattern SOT_PATH_PAYMENT = 2
pattern SOT_MANAGE_OFFER = 3
pattern SOT_CREATE_PASSIVE_OFFER = 4
pattern SOT_SET_OPTIONS = 5
pattern SOT_CHANGE_TRUST = 6
pattern SOT_ALLOW_TRUST = 7
pattern SOT_ACCOUNT_MERGE = 8
pattern SOT_INFLATION = 9
pattern SOT_MANAGE_DATA = 10
pattern SOT_BUMP_SEQUENCE = 11
pattern SOT_MANAGE_BUY_OFFER = 12
pattern SOT_PATH_PAYMENT_STRICT_SEND = 13

pattern ASSET_TYPE_NATIVE = 0
pattern ASSET_TYPE_CREDIT_ALPHANUM4 = 1
pattern ASSET_TYPE_CREDIT_ALPHANUM12 = 2

pattern TX_SUCCESS = 0
pattern TX_FAILED = -1
-- pattern TX_TOO_EARLY = -2
-- pattern TX_TOO_LATE = -3
-- pattern TX_MISSING_OPERATION = -4
-- pattern TX_BAD_SEQ = -5
-- pattern TX_BAD_AUTH = -6
-- pattern TX_INSUFFICIENT_BALANCE = -7
-- pattern TX_NO_ACCOUNT = -8
-- pattern TX_INSUFFICIENT_FEE = -9
-- pattern TX_BAD_AUTH_EXTRA = -10
-- pattern TX_INTERNAL_ERROR = -11

pattern OP_INNER = 0
-- pattern OP_BAD_AUTH = -1
-- pattern OP_NO_ACCOUNT = -2
-- pattern OP_NOT_SUPPORTED = -3

pattern PATH_PAYMENT_SUCCESS = 0
-- pattern PATH_PAYMENT_MALFORMED = -1
-- pattern PATH_PAYMENT_UNDERFUNDED = -2
-- pattern PATH_PAYMENT_SRC_NO_TRUST = -3
-- pattern PATH_PAYMENT_SRC_NOT_AUTHORIZED = -4
-- pattern PATH_PAYMENT_NO_DESTINATION = -5
-- pattern PATH_PAYMENT_NO_TRUST = -6
-- pattern PATH_PAYMENT_NOT_AUTHORIZED = -7
-- pattern PATH_PAYMENT_LINE_FULL = -8
pattern PATH_PAYMENT_NO_ISSUER = -9
-- pattern PATH_PAYMENT_TOO_FEW_OFFERS = -10
-- pattern PATH_PAYMENT_OFFER_CROSS_SELF = -11
-- pattern PATH_PAYMENT_OVER_SENDMAX = -12

pattern MANAGE_OFFER_SUCCESS = 0
-- pattern MANAGE_OFFER_MALFORMED = -1
-- pattern MANAGE_OFFER_SELL_NO_TRUST = -2
-- pattern MANAGE_OFFER_BUY_NO_TRUST = -3
-- pattern MANAGE_OFFER_SELL_NOT_AUTHORIZED = -4
-- pattern MANAGE_OFFER_BUY_NOT_AUTHORIZED = -5
-- pattern MANAGE_OFFER_LINE_FULL = -6
-- pattern MANAGE_OFFER_UNDERFUNDED = -7
-- pattern MANAGE_OFFER_CROSS_SELF = -8
-- pattern MANAGE_OFFER_SELL_NO_ISSUER = -9
-- pattern MANAGE_OFFER_BUY_NO_ISSUER = -10
-- pattern MANAGE_OFFER_NOT_FOUND = -11
-- pattern MANAGE_OFFER_LOW_RESERVE = -12

pattern ACCOUNT_MERGE_SUCCESS = 0
-- pattern ACCOUNT_MERGE_MALFORMED = -1
-- pattern ACCOUNT_MERGE_NO_ACCOUNT = -2
-- pattern ACCOUNT_MERGE_IMMUTABLE_SET = -3
-- pattern ACCOUNT_MERGE_HAS_SUB_ENTRIES = -4
-- pattern ACCOUNT_MERGE_SEQNUM_TOO_FAR = -5
-- pattern ACCOUNT_MERGE_DEST_FULL = -6

pattern INFLATION_SUCCESS = 0
-- pattern INFLATION_NOT_TIME = -1

pattern MANAGE_OFFER_CREATED = 0
pattern MANAGE_OFFER_UPDATED = 1
-- pattern MANAGE_OFFER_DELETED = 2

pattern MANAGE_BUY_OFFER_SUCCESS = 0

pattern PATH_PAYMENT_STRICT_SEND_SUCCESS = 0
-- pattern PATH_PAYMENT_STRICT_SEND_MALFORMED = -1
-- pattern PATH_PAYMENT_STRICT_SEND_UNDERFUNDED = -2
-- pattern PATH_PAYMENT_STRICT_SEND_SRC_NO_TRUST = -3
-- pattern PATH_PAYMENT_STRICT_SEND_SRC_NOT_AUTHORIZED = -4
-- pattern PATH_PAYMENT_STRICT_SEND_NO_DESTINATION = -5
-- pattern PATH_PAYMENT_STRICT_SEND_NO_TRUST = -6
-- pattern PATH_PAYMENT_STRICT_SEND_NOT_AUTHORIZED = -7
-- pattern PATH_PAYMENT_STRICT_SEND_LINE_FULL = -8
pattern PATH_PAYMENT_STRICT_SEND_NO_ISSUER = -9
-- pattern PATH_PAYMENT_STRICT_SEND_TOO_FEW_OFFERS = -10
-- pattern PATH_PAYMENT_STRICT_SEND_OFFER_CROSS_SELF = -11
-- pattern PATH_PAYMENT_STRICT_SEND_UNDER_DESTMIN = -12

-- pattern ENVELOPE_TYPE_SCP = 1
pattern ENVELOPE_TYPE_TX = 2
-- pattern ENVELOPE_TYPE_AUTH = 3


data StellarAsset = StellarAsset
  { sa_assetCode :: !(Maybe T.Text)
  , sa_issuer :: !(Maybe HexString)
  }

data TransactionResult = TransactionResult
  { tr_feeCharded :: {-# UNPACK #-} !Int64
  , tr_code :: {-# UNPACK #-} !Int64
  , tr_results :: !(V.Vector OperationResult)
  }

data OperationResult = OperationResult
  { or_code :: {-# UNPACK #-} !Int64
  , or_type :: !(Maybe Int64)
  , or_result :: !(Maybe OpResult)
  , or_destination :: !(Maybe HexString)
  , or_asset :: !(Maybe StellarAsset)
  , or_amount :: !(Maybe Int64)
  , or_effect :: !(Maybe Int64)
  , or_claimOffers :: !(V.Vector StellarClaimOfferAtom)
  , or_offer :: !(Maybe StellarOfferEntry)
  , or_inflationPayouts :: !(V.Vector StellarInflationPayout)
  }

instance Default OperationResult

type OpResult = Int64

genSchemaInstances [''StellarLedger, ''StellarTransaction, ''StellarOperation, ''StellarClaimOfferAtom, ''StellarOfferEntry, ''StellarInflationPayout, ''StellarAsset, ''TransactionResult, ''OperationResult]


parseLedgers :: BL.ByteString -> BL.ByteString -> BL.ByteString -> IO [StellarLedger]
parseLedgers ledgersBytes transactionsBytes resultsBytes = do
  ledgers <- either fail return $ S.runGetLazy (getSequence getLedgerHeaderHistoryEntry) ledgersBytes
  transactions <- either fail return $ S.runGetLazy (getSequence getTransactionHistoryEntry) transactionsBytes
  results <- either fail return $ S.runGetLazy (getSequence getTransactionHistoryResultEntry) resultsBytes
  forM ledgers $ \ledger@StellarLedger
    { sl_sequence = ledgerSeq
    } -> do
    let
      ledgerTransactions = mconcat $ map snd $ filter ((== ledgerSeq) . fst) transactions
      ledgerResults = HM.fromList $ zipWith (\i (h, l) -> (h, (i, l))) [0..] $ V.toList $ mconcat $ map snd $ filter ((== ledgerSeq) . fst) results
    unless (V.length ledgerTransactions == HM.size ledgerResults) $ fail "transactions do not correspond to results"
    ledgerTransactionsWithResults <- forM ledgerTransactions $ \tx@(flip HM.lookup ledgerResults . st_hash -> Just (applyIndex, result)) -> combineTransactionAndResult tx
      { st_applyIndex = applyIndex
      } result
    return ledger
      { sl_transactions = ledgerTransactionsWithResults
      }
  where
    getLedgerHeaderHistoryEntry :: S.Get StellarLedger
    getLedgerHeaderHistoryEntry = do
      S.skip 4
      hash <- getHash
      ledgerHeader <- getLedgerHeader
      getExt
      return ledgerHeader
        { sl_hash = hash
        }

    getLedgerHeader :: S.Get StellarLedger
    getLedgerHeader = do
      _ledgerVersion <- S.getWord32be
      _previousLedgerHash <- getHash
      (_txSetHash, closeTime) <- getStellarValue
      _txSetResultHash <- getHash
      _bucketListHash <- getHash
      ledgerSeq <- fromIntegral <$> S.getWord32be
      totalCoins <- fromIntegral <$> S.getInt64be
      feePool <- fromIntegral <$> S.getInt64be
      inflationSeq <- fromIntegral <$> S.getWord32be
      idPool <- fromIntegral <$> S.getWord64be
      baseFee <- fromIntegral <$> S.getWord32be
      baseReserve <- fromIntegral <$> S.getWord32be
      maxTxSetSize <- fromIntegral <$> S.getWord32be
      _skipList <- replicateM 4 getHash
      getExt
      return StellarLedger
        { sl_sequence = ledgerSeq
        , sl_hash = mempty
        , sl_closeTime = closeTime
        , sl_totalCoins = totalCoins
        , sl_feePool = feePool
        , sl_inflationSeq = inflationSeq
        , sl_idPool = idPool
        , sl_baseFee = baseFee
        , sl_baseReserve = baseReserve
        , sl_maxTxSetSize = maxTxSetSize
        , sl_transactions = V.empty
        }

    getTransactionHistoryEntry :: S.Get (Int64, V.Vector StellarTransaction)
    getTransactionHistoryEntry = do
      S.skip 4
      ledgerSeq <- fromIntegral <$> S.getWord32be
      txSet <- getTransactionSet
      getExt
      return (ledgerSeq, txSet)

    getTransactionHistoryResultEntry :: S.Get (Int64, V.Vector (HexString, TransactionResult))
    getTransactionHistoryResultEntry = do
      S.skip 4
      ledgerSeq <- fromIntegral <$> S.getWord32be
      txResultSet <- getTransactionResultSet
      getExt
      return (ledgerSeq, txResultSet)

    combineTransactionAndResult :: StellarTransaction -> TransactionResult -> IO StellarTransaction
    combineTransactionAndResult transaction@StellarTransaction
      { st_operations = operations
      } TransactionResult
      { tr_feeCharded = feeCharged
      , tr_code = txResultCode
      , tr_results = opResults
      } = do
      -- some results are missing, allow that
      unless (V.length operations == V.length opResults || V.length opResults == 0) $ fail "operations do not correspond to results"
      combinedOperations <- V.zipWithM combineOperationAndResult operations opResults
      return transaction
        { st_feeCharged = feeCharged
        , st_resultCode = txResultCode
        , st_operations = combinedOperations
        }

    combineOperationAndResult :: StellarOperation -> OperationResult -> IO StellarOperation
    combineOperationAndResult operation@StellarOperation
      { so_type = opType
      } OperationResult
      { or_code = opResultCode
      , or_type = opResultOpType
      , or_result = opResultResult
      , or_destination = opResultDestination
      , or_asset = opResultAsset
      , or_amount = opResultAmount
      , or_effect = opResultEffect
      , or_claimOffers = opResultClaimOffers
      , or_offer = opResultOffer
      , or_inflationPayouts = opResultInflationPayouts
      } = do
      -- allow operation be "create passive offer" and result be "manage offer"; it happens quite frequently
      let
        opTypesEqual ot rot = (ot == rot) || (ot == SOT_CREATE_PASSIVE_OFFER && rot == SOT_MANAGE_OFFER)
        in unless (maybe True (opTypesEqual opType) opResultOpType) $ fail "operation does not correspond to result"
      return operation
        { so_resultCode = Just opResultCode
        , so_result = opResultResult
        , so_resultDestination = opResultDestination
        , so_resultAsset = opResultAsset
        , so_resultAmount = opResultAmount
        , so_resultEffect = opResultEffect
        , so_resultClaimOffers = opResultClaimOffers
        , so_resultOffer = opResultOffer
        , so_resultInflationPayouts = opResultInflationPayouts
        }

    getTransactionSet :: S.Get (V.Vector StellarTransaction)
    getTransactionSet = do
      _previousLedgerHash <- getHash
      getArray getTransactionEnvelope

    getTransactionResultSet :: S.Get (V.Vector (HexString, TransactionResult))
    getTransactionResultSet = getArray getTransactionResultPair

    getTransactionResultPair :: S.Get (HexString, TransactionResult)
    getTransactionResultPair = do
      txHash <- getHash
      txResult <- getTransactionResult
      return (txHash, txResult)

    getTransactionEnvelope :: S.Get StellarTransaction
    getTransactionEnvelope = do
      (txBytes, tx) <- alsoBytes getTransaction
      -- calculate hash
      let
        txHash = let
          payload = S.runPut $ do -- this is TransactionSignaturePayload struct
            S.putByteString productionNetworkId
            S.putWord32be ENVELOPE_TYPE_TX
            S.putByteString txBytes
          in HexString $ BS.toShort $ BA.convert (C.hash payload :: C.Digest C.SHA256)
      _signatures <- getArray getDecoratedSignature
      return tx
        { st_hash = txHash
        }

    getTransaction :: S.Get StellarTransaction
    getTransaction = do
      sourceAccount <- getAccountID
      fee <- fromIntegral <$> S.getWord32be
      seqNum <- getSequenceNumber
      timeBounds <- getMaybe getTimeBounds
      _memo <- getMemo
      operations <- getArray getOperation
      getExt
      return StellarTransaction
        { st_hash = mempty
        , st_applyIndex = -1
        , st_sourceAccount = sourceAccount
        , st_fee = fee
        , st_seqNum = seqNum
        , st_timeBoundsMinTime = fst <$> timeBounds
        , st_timeBoundsMaxTime = snd <$> timeBounds
        , st_feeCharged = 0
        , st_resultCode = -0x7fffffff
        , st_operations = operations
        }

    getTransactionResult :: S.Get TransactionResult
    getTransactionResult = do
      feeCharged <- S.getInt64be
      code <- fromIntegral <$> S.getInt32be
      results <- if code == TX_SUCCESS || code == TX_FAILED
        then getArray getOperationResult
        else return mempty
      getExt
      return TransactionResult
        { tr_feeCharded = feeCharged
        , tr_code = code
        , tr_results = results
        }

    getOperation :: S.Get StellarOperation
    getOperation = do
      sourceAccount <- getMaybe getAccountID
      opType <- getOperationType
      op <- case opType of
        SOT_CREATE_ACCOUNT -> getCreateAccountOp
        SOT_PAYMENT -> getPaymentOp
        SOT_PATH_PAYMENT -> getPathPaymentOp
        SOT_MANAGE_OFFER -> getManageOfferOp
        SOT_CREATE_PASSIVE_OFFER -> getCreatePassiveOfferOp
        SOT_SET_OPTIONS -> getSetOptionsOp
        SOT_CHANGE_TRUST -> getChangeTrustOp
        SOT_ALLOW_TRUST -> getAllowTrustOp
        SOT_ACCOUNT_MERGE -> do
          destination <- getAccountID
          return def
            { so_destination = Just destination
            }
        SOT_INFLATION -> return def
        SOT_MANAGE_DATA -> getManageDataOp
        SOT_BUMP_SEQUENCE -> getBumpSequenceOp
        SOT_MANAGE_BUY_OFFER -> getManageBuyOfferOp
        SOT_PATH_PAYMENT_STRICT_SEND -> getPathPaymentStrictSendOp
        _ -> fail $ "wrong op type: " <> show opType
      return op
        { so_type = opType
        , so_sourceAccount = sourceAccount
        }

    getOperationType :: S.Get Int64
    getOperationType = fromIntegral <$> S.getWord32be

    getCreateAccountOp :: S.Get StellarOperation
    getCreateAccountOp = do
      destination <- getAccountID
      startingBalance <- S.getInt64be
      return def
        { so_destination = Just destination
        , so_startingBalance = Just startingBalance
        }

    getPaymentOp :: S.Get StellarOperation
    getPaymentOp = do
      destination <- getAccountID
      asset <- getAsset
      amount <- S.getInt64be
      return def
        { so_destination = Just destination
        , so_asset = Just asset
        , so_amount = Just amount
        }

    getPathPaymentOp :: S.Get StellarOperation
    getPathPaymentOp = do
      sendAsset <- getAsset
      sendMax <- S.getInt64be
      destination <- getAccountID
      destAsset <- getAsset
      destAmount <- S.getInt64be
      path <- getArray getAsset
      return def
        { so_sendAsset = Just sendAsset
        , so_sendMax = Just sendMax
        , so_destination = Just destination
        , so_destAsset = Just destAsset
        , so_destAmount = Just destAmount
        , so_path = path
        }

    getManageOfferOp :: S.Get StellarOperation
    getManageOfferOp = do
      selling <- getAsset
      buying <- getAsset
      amount <- S.getInt64be
      price <- getPrice
      offerID <- fromIntegral <$> S.getWord64be
      return def
        { so_selling = Just selling
        , so_buying = Just buying
        , so_amount = Just amount
        , so_price = Just price
        , so_offerID = Just offerID
        }

    getCreatePassiveOfferOp :: S.Get StellarOperation
    getCreatePassiveOfferOp = do
      selling <- getAsset
      buying <- getAsset
      amount <- S.getInt64be
      price <- getPrice
      return def
        { so_selling = Just selling
        , so_buying = Just buying
        , so_amount = Just amount
        , so_price = Just price
        }

    getSetOptionsOp :: S.Get StellarOperation
    getSetOptionsOp = do
      inflationDest <- getMaybe getAccountID
      clearFlags <- getMaybe S.getWord32be
      setFlags <- getMaybe S.getWord32be
      masterWeight <- getMaybe S.getWord32be
      lowThreshold <- getMaybe S.getWord32be
      medThreshold <- getMaybe S.getWord32be
      highThreshold <- getMaybe S.getWord32be
      homeDomain <- getMaybe getString
      _signer <- getMaybe getSigner
      return def
        { so_inflationDest = inflationDest
        , so_clearFlags = fromIntegral <$> clearFlags
        , so_setFlags = fromIntegral <$> setFlags
        , so_masterWeight = fromIntegral <$> masterWeight
        , so_lowThreshold = fromIntegral <$> lowThreshold
        , so_medThreshold = fromIntegral <$> medThreshold
        , so_highThreshold = fromIntegral <$> highThreshold
        , so_homeDomain = homeDomain
        }

    getChangeTrustOp :: S.Get StellarOperation
    getChangeTrustOp = do
      line <- getAsset
      limit <- S.getInt64be
      return def
        { so_line = Just line
        , so_limit = Just limit
        }

    getAllowTrustOp :: S.Get StellarOperation
    getAllowTrustOp = do
      trustor <- getAccountID
      assetType <- S.getWord32be
      assetCode <- case assetType of
        ASSET_TYPE_CREDIT_ALPHANUM4 -> T.decodeUtf8 . trimZeros <$> S.getBytes 4
        ASSET_TYPE_CREDIT_ALPHANUM12 -> T.decodeUtf8 . trimZeros <$> S.getBytes 12
        _ -> fail "wrong asset type"
      authorize <- getBool
      return def
        { so_trustor = Just trustor
        , so_assetCode = Just assetCode
        , so_authorize = Just authorize
        }

    getManageDataOp :: S.Get StellarOperation
    getManageDataOp = do
      dataName <- getString
      dataValue <- getMaybe getOpaque
      return def
        { so_dataName = Just dataName
        , so_dataValue = dataValue
        }

    getBumpSequenceOp :: S.Get StellarOperation
    getBumpSequenceOp = do
      bumpTo <- getSequenceNumber
      return def
        { so_bumpTo = Just bumpTo
        }

    getManageBuyOfferOp :: S.Get StellarOperation
    getManageBuyOfferOp = do
      selling <- getAsset
      buying <- getAsset
      amount <- S.getInt64be
      price <- getPrice
      offerID <- S.getInt64be
      return def
        { so_selling = Just selling
        , so_buying = Just buying
        , so_amount = Just amount
        , so_price = Just price
        , so_offerID = Just offerID
        }

    getPathPaymentStrictSendOp :: S.Get StellarOperation
    getPathPaymentStrictSendOp = do
      sendAsset <- getAsset
      sendAmount <- S.getInt64be
      destination <- getAccountID
      destAsset <- getAsset
      destMin <- S.getInt64be
      path <- getArray getAsset
      return def
        { so_sendAsset = Just sendAsset
        , so_amount = Just sendAmount
        , so_destination = Just destination
        , so_destAsset = Just destAsset
        , so_destAmount = Just destMin
        , so_path = path
        }

    getOperationResult :: S.Get OperationResult
    getOperationResult = do
      code <- S.getInt32be
      case code of
        OP_INNER -> do
          opType <- getOperationType
          opResult <- case opType of
            SOT_CREATE_ACCOUNT -> getCreateAccountResult
            SOT_PAYMENT -> getPaymentResult
            SOT_PATH_PAYMENT -> getPathPaymentResult
            SOT_MANAGE_OFFER -> getManageOfferResult
            SOT_CREATE_PASSIVE_OFFER -> getManageOfferResult
            SOT_SET_OPTIONS -> getSetOptionsResult
            SOT_CHANGE_TRUST -> getChangeTrustResult
            SOT_ALLOW_TRUST -> getAllowTrustResult
            SOT_ACCOUNT_MERGE -> getAccountMergeResult
            SOT_INFLATION -> getInflationResult
            SOT_MANAGE_DATA -> getManageDataResult
            SOT_BUMP_SEQUENCE -> getBumpSequenceResult
            SOT_MANAGE_BUY_OFFER -> getManageBuyOfferResult
            SOT_PATH_PAYMENT_STRICT_SEND -> getPathPaymentStrictSendResult
            _ -> fail "wrong op type"
          return opResult
            { or_code = fromIntegral code
            , or_type = Just opType
            }
        _ -> return def
          { or_code = fromIntegral code
          }

    getCreateAccountResult :: S.Get OperationResult
    getCreateAccountResult = getOpResult

    getPaymentResult :: S.Get OperationResult
    getPaymentResult = getOpResult

    getPathPaymentResult :: S.Get OperationResult
    getPathPaymentResult = do
      opResult@OperationResult
        { or_result = Just resultCode
        } <- getOpResult
      case resultCode of
        PATH_PAYMENT_SUCCESS -> do
          claimOffers <- getArray getClaimOfferAtom
          (destination, asset, amount) <- getSimplePaymentResult
          return opResult
            { or_destination = Just destination
            , or_asset = Just asset
            , or_amount = Just amount
            , or_claimOffers = claimOffers
            }
        PATH_PAYMENT_NO_ISSUER -> do
          void getAsset
          return opResult
        _ -> return opResult

    getManageOfferResult :: S.Get OperationResult
    getManageOfferResult = do
      opResult@OperationResult
        { or_result = Just resultCode
        } <- getOpResult
      case resultCode of
        MANAGE_OFFER_SUCCESS -> getManageOfferSuccessResult opResult
        _ -> return opResult

    getSetOptionsResult :: S.Get OperationResult
    getSetOptionsResult = getOpResult

    getChangeTrustResult :: S.Get OperationResult
    getChangeTrustResult = getOpResult

    getAllowTrustResult :: S.Get OperationResult
    getAllowTrustResult = getOpResult

    getAccountMergeResult :: S.Get OperationResult
    getAccountMergeResult = do
      opResult@OperationResult
        { or_result = Just resultCode
        } <- getOpResult
      case resultCode of
        ACCOUNT_MERGE_SUCCESS -> do
          _sourceAccountBalance <- S.getInt64be
          return ()
        _ -> return ()
      return opResult

    getInflationResult :: S.Get OperationResult
    getInflationResult = do
      opResult@OperationResult
        { or_result = Just resultCode
        } <- getOpResult
      case resultCode of
        INFLATION_SUCCESS -> do
          inflationPayouts <- getArray getInflationPayout
          return opResult
            { or_inflationPayouts = inflationPayouts
            }
        _ -> return opResult

    getManageDataResult :: S.Get OperationResult
    getManageDataResult = getOpResult

    getBumpSequenceResult :: S.Get OperationResult
    getBumpSequenceResult = getOpResult

    getManageBuyOfferResult :: S.Get OperationResult
    getManageBuyOfferResult = do
      opResult@OperationResult
        { or_result = Just resultCode
        } <- getOpResult
      case resultCode of
        MANAGE_BUY_OFFER_SUCCESS -> getManageOfferSuccessResult opResult
        _ -> return opResult

    getPathPaymentStrictSendResult :: S.Get OperationResult
    getPathPaymentStrictSendResult = do
      opResult@OperationResult
        { or_result = Just resultCode
        } <- getOpResult
      case resultCode of
        PATH_PAYMENT_STRICT_SEND_SUCCESS -> do
          claimOffers <- getArray getClaimOfferAtom
          (destination, asset, amount) <- getSimplePaymentResult
          return opResult
            { or_destination = Just destination
            , or_asset = Just asset
            , or_amount = Just amount
            , or_claimOffers = claimOffers
            }
        PATH_PAYMENT_STRICT_SEND_NO_ISSUER -> do
          void getAsset
          return opResult
        _ -> return opResult

    getManageOfferSuccessResult :: OperationResult -> S.Get OperationResult
    getManageOfferSuccessResult opResult = do
      claimOffers <- getArray getClaimOfferAtom
      effect <- S.getInt32be
      offer <- case effect of
        MANAGE_OFFER_CREATED -> Just <$> getOfferEntry
        MANAGE_OFFER_UPDATED -> Just <$> getOfferEntry
        _ -> return Nothing
      return opResult
        { or_effect = Just $ fromIntegral effect
        , or_claimOffers = claimOffers
        , or_offer = offer
        }

    getOpResult :: S.Get OperationResult
    getOpResult = do
      opResult <- fromIntegral <$> S.getInt32be
      return def
        { or_result = Just opResult
        }

    getSimplePaymentResult :: S.Get (HexString, StellarAsset, Int64)
    getSimplePaymentResult = do
      destination <- getAccountID
      asset <- getAsset
      amount <- S.getInt64be
      return (destination, asset, amount)

    getInflationPayout :: S.Get StellarInflationPayout
    getInflationPayout = do
      destination <- getAccountID
      amount <- S.getInt64be
      return StellarInflationPayout
        { sip_destination = destination
        , sip_amount = amount
        }

    getSigner :: S.Get ()
    getSigner = do
      _key <- getSignerKey
      _weight <- S.getWord32be
      return ()

    getSignerKey :: S.Get ()
    getSignerKey = do
      _type <- S.getWord32be
      S.skip 32

    getClaimOfferAtom :: S.Get StellarClaimOfferAtom
    getClaimOfferAtom = StellarClaimOfferAtom
      <$> getAccountID
      <*> S.getInt64be
      <*> getAsset
      <*> S.getInt64be
      <*> getAsset
      <*> S.getInt64be

    getOfferEntry :: S.Get StellarOfferEntry
    getOfferEntry = do
      sellerID <- getAccountID
      offerID <- fromIntegral <$> S.getWord64be
      selling <- getAsset
      buying <- getAsset
      amount <- S.getInt64be
      price <- getPrice
      flags <- fromIntegral <$> S.getWord32be
      getExt
      return StellarOfferEntry
        { soe_sellerID = sellerID
        , soe_offerID = offerID
        , soe_selling = selling
        , soe_buying = buying
        , soe_amount = amount
        , soe_price = price
        , soe_flags = flags
        }

    getStellarValue :: S.Get (HexString, Int64)
    getStellarValue = do
      txSetHash <- getHash
      closeTime <- fromIntegral <$> S.getWord64be
      _upgrades <- getArray getOpaque
      getExt
      return (txSetHash, closeTime)

    getAccountID :: S.Get HexString
    getAccountID = getPublicKey

    getPublicKey :: S.Get HexString
    getPublicKey = do
      _type <- S.getWord32be
      HexString . BS.toShort <$> S.getBytes 32

    getHash :: S.Get HexString
    getHash = HexString . BS.toShort <$> S.getBytes 32

    getAsset :: S.Get StellarAsset
    getAsset = do
      assetType <- S.getWord32be
      case assetType of
        ASSET_TYPE_NATIVE -> return StellarAsset
          { sa_assetCode = Nothing
          , sa_issuer = Nothing
          }
        ASSET_TYPE_CREDIT_ALPHANUM4 -> do
          assetCode <- T.decodeUtf8 . trimZeros <$> S.getBytes 4
          issuer <- getAccountID
          return StellarAsset
            { sa_assetCode = Just assetCode
            , sa_issuer = Just issuer
            }
        ASSET_TYPE_CREDIT_ALPHANUM12 -> do
          assetCode <- T.decodeUtf8 . trimZeros <$> S.getBytes 12
          issuer <- getAccountID
          return StellarAsset
            { sa_assetCode = Just assetCode
            , sa_issuer = Just issuer
            }
        _ -> fail "wrong asset type"

    getOpaque :: S.Get HexString
    getOpaque = do
      size <- fromIntegral <$> S.getWord32be
      bytes <- HexString . BS.toShort <$> S.getBytes size
      S.skip $ (4 - size `rem` 4) .&. 3
      return bytes

    getPrice :: S.Get Double
    getPrice = do
      n <- S.getInt32be
      d <- S.getInt32be
      return $ fromIntegral n / fromIntegral d

    getDecoratedSignature :: S.Get ()
    getDecoratedSignature = do
      _hint <- S.getBytes 4
      _signature <- getOpaque
      return ()

    getSequenceNumber :: S.Get Int64
    getSequenceNumber = S.getInt64be

    getExt :: S.Get ()
    getExt = do
      ext <- S.getWord32be
      unless (ext == 0) $ fail "non-zero ext"

    getSequence :: S.Get a -> S.Get [a]
    getSequence g = do
      isEmpty <- S.isEmpty
      if isEmpty then return [] else do
        r <- g
        (r :) <$> getSequence g

    getArray :: S.Get a -> S.Get (V.Vector a)
    getArray g = do
      size <- S.getWord32be
      V.replicateM (fromIntegral size) g

    getString :: S.Get T.Text
    getString = do
      size <- fromIntegral <$> S.getWord32be
      bytes <- S.getBytes size
      S.skip $ (4 - size `rem` 4) .&. 3
      return $ T.decodeUtf8 bytes

    getBool :: S.Get Bool
    getBool = (> 0) <$> S.getWord32be

    getTimeBounds :: S.Get (Int64, Int64)
    getTimeBounds = do
      minTime <- fromIntegral <$> S.getWord64be
      maxTime <- fromIntegral <$> S.getWord64be
      return (minTime, maxTime)

    getMemo :: S.Get ()
    getMemo = do
      memoType <- S.getWord32be
      case memoType of
        0 -> return ()
        1 -> void getString
        2 -> void S.getWord64be
        3 -> void getHash
        4 -> void getHash
        _ -> fail "wrong memo"

    getMaybe :: S.Get a -> S.Get (Maybe a)
    getMaybe g = do
      i <- S.getWord32be
      case i of
        0 -> return Nothing
        1 -> Just <$> g
        _ -> fail "wrong optional field"

    trimZeros :: B.ByteString -> B.ByteString
    trimZeros = fst . B.spanEnd (== 0)

    alsoBytes :: S.Get a -> S.Get (B.ByteString, a)
    alsoBytes m = do
      (len, r) <- S.lookAhead $ do
        i <- S.bytesRead
        r <- m
        j <- S.bytesRead
        return (j - i, r)
      bytes <- S.getBytes len
      return (bytes, r)


instance Default (V.Vector a) where
  def = V.empty

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
      parseLedgers ledgersBytes transactionsBytes resultsBytes
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
      [ schemaOf (Proxy :: Proxy StellarAsset)
      , schemaOf (Proxy :: Proxy StellarInflationPayout)
      , schemaOf (Proxy :: Proxy StellarOfferEntry)
      , schemaOf (Proxy :: Proxy StellarClaimOfferAtom)
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
