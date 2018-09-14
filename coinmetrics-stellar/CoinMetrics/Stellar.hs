{-# LANGUAGE DeriveGeneric, OverloadedStrings, PatternSynonyms, TemplateHaskell, TypeFamilies, ViewPatterns #-}
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
import qualified Data.Aeson as J
import qualified Data.Aeson.Types as J
import Data.Bits
import qualified Data.ByteString as B
import qualified Data.ByteString.Lazy as BL
import qualified Data.ByteString.Short as BS
import Data.Default
import Data.Int
import Data.Proxy
import qualified Data.Serialize as S
import qualified Data.Text as T
import qualified Data.Text.Encoding as T
import qualified Data.Vector as V
import GHC.Generics(Generic)
import qualified Network.HTTP.Client as H
import Numeric
import System.IO.Unsafe(unsafeInterleaveIO)

import CoinMetrics.BlockChain
import CoinMetrics.Schema.Util
import CoinMetrics.Util
import Hanalytics.Schema

-- | Stellar connector.
data Stellar = Stellar
	{ stellar_httpManager :: !H.Manager
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
	} deriving Generic

data StellarTransaction = StellarTransaction
	{ st_sourceAccount :: {-# UNPACK #-} !HexString
	, st_fee :: {-# UNPACK #-} !Int64
	, st_seqNum :: {-# UNPACK #-} !Int64
	, st_timeBoundsMinTime :: !(Maybe Int64)
	, st_timeBoundsMaxTime :: !(Maybe Int64)
	, st_operations :: !(V.Vector StellarOperation)
	} deriving Generic

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
	} deriving Generic

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

pattern ASSET_TYPE_NATIVE = 0
pattern ASSET_TYPE_CREDIT_ALPHANUM4 = 1
pattern ASSET_TYPE_CREDIT_ALPHANUM12 = 2

data StellarAsset = StellarAsset
	{ sa_assetCode :: !(Maybe T.Text)
	, sa_issuer :: !(Maybe HexString)
	} deriving Generic

genSchemaInstances [''StellarLedger, ''StellarTransaction, ''StellarOperation, ''StellarAsset]

parseLedgers :: BL.ByteString -> BL.ByteString -> IO [StellarLedger]
parseLedgers ledgersBytes transactionsBytes = do
	ledgers <- either fail return $ S.runGetLazy (getSequence getLedgerHeaderHistoryEntry) ledgersBytes
	transactions <- either fail return $ S.runGetLazy (getSequence getTransactionHistoryEntry) transactionsBytes
	return $ flip map ledgers $ \ledger@StellarLedger
		{ sl_sequence = ledgerSeq
		} -> ledger
		{ sl_transactions = mconcat $ map snd $ filter ((== ledgerSeq) . fst) transactions
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
			scpValue <- getStellarValue
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
			_skipList <- replicateM 4 $ getHash
			getExt
			return StellarLedger
				{ sl_sequence = ledgerSeq
				, sl_hash = mempty
				, sl_closeTime = scpValue
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

		getTransactionSet :: S.Get (V.Vector StellarTransaction)
		getTransactionSet = do
			_previousLedgerHash <- getHash
			getArray getTransactionEnvelope

		getTransactionEnvelope :: S.Get StellarTransaction
		getTransactionEnvelope = do
			tx <- getTransaction
			_signatures <- getArray getDecoratedSignature
			return tx

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
				{ st_sourceAccount = sourceAccount
				, st_fee = fee
				, st_seqNum = seqNum
				, st_timeBoundsMinTime = fst <$> timeBounds
				, st_timeBoundsMaxTime = snd <$> timeBounds
				, st_operations = operations
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
				_ -> fail "wrong op type"
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

		getSigner :: S.Get ()
		getSigner = do
			_key <- getSignerKey
			_weight <- S.getWord32be
			return ()

		getSignerKey :: S.Get ()
		getSignerKey = do
			_type <- S.getWord32be
			S.skip 32

		getStellarValue :: S.Get Int64
		getStellarValue = do
			_txSetHash <- getHash
			closeTime <- fromIntegral <$> S.getWord64be
			_upgrades <- getArray getOpaque
			getExt
			return closeTime

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

instance Default (V.Vector a) where
	def = V.empty

stellarRequest :: Stellar -> T.Text -> IO BL.ByteString
stellarRequest Stellar
	{ stellar_httpManager = httpManager
	, stellar_httpRequest = httpRequest
	} path = tryWithRepeat $ H.responseBody <$> H.httpLbs httpRequest
	{ H.path = H.path httpRequest <> T.encodeUtf8 path
	} httpManager

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
			ledgersBytes <- GZip.decompress <$> stellarRequest stellar ledgerPath
			transactionsBytes <- GZip.decompress <$> stellarRequest stellar transactionsPath
			parseLedgers ledgersBytes transactionsBytes
		sequenceHex = showHex checkpointSequence ""
		sequenceHexPadded@[h0, h1, h2, h3, h4, h5, _h6, _h7]
			= replicate (max 0 (8 - length sequenceHex)) '0' ++ sequenceHex
		ledgerPath = "/ledger/" <> T.pack [h0, h1, '/', h2, h3, '/', h4, h5] <> "/ledger-" <> T.pack sequenceHexPadded <> ".xdr.gz"
		transactionsPath = "/transactions/" <> T.pack [h0, h1, '/', h2, h3, '/', h4, h5] <> "/transactions-" <> T.pack sequenceHexPadded <> ".xdr.gz"

instance BlockChain Stellar where
	type Block Stellar = StellarLedger

	getBlockChainInfo _ = BlockChainInfo
		{ bci_init = \BlockChainParams
			{ bcp_httpManager = httpManager
			, bcp_httpRequest = httpRequest
			, bcp_threadsCount = threadsCount
			} -> do
			checkpointCacheVar <- newTVarIO []
			return Stellar
				{ stellar_httpManager = httpManager
				, stellar_httpRequest = httpRequest
				, stellar_checkpointCacheVar = checkpointCacheVar
				, stellar_checkpointCacheSize = 2 + threadsCount `quot` 64
				}
		, bci_defaultApiUrl = "http://history.stellar.org/prd/core-live/core_live_001"
		, bci_defaultBeginBlock = 1
		, bci_defaultEndBlock = 0 -- history data, no rewrites
		, bci_schemas = standardBlockChainSchemas
			(schemaOf (Proxy :: Proxy StellarLedger))
			[ schemaOf (Proxy :: Proxy StellarAsset)
			, schemaOf (Proxy :: Proxy StellarOperation)
			, schemaOf (Proxy :: Proxy StellarTransaction)
			]
			"CREATE TABLE \"stellar\" OF \"StellarLedger\" (PRIMARY KEY (\"sequence\"));"
		}

	getCurrentBlockHeight stellar = either fail return
		. (J.parseEither (J..: "currentLedger") <=< J.eitherDecode)
		=<< stellarRequest stellar "/.well-known/stellar-history.json"

	getBlockByHeight stellar blockHeight = do
		ledgers <- filter ((== blockHeight) . sl_sequence) <$>
			withCheckpointCache stellar (((blockHeight + 64) .&. complement 0x3f) - 1)
		case ledgers of
			[ledger] -> return ledger
			_ -> fail "ledger cache error"

	blockHeightFieldName _ = "sequence"
