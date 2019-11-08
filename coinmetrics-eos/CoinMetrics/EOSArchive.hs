{-# LANGUAGE DefaultSignatures #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE StandaloneDeriving #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE TypeOperators #-}
{-# LANGUAGE ViewPatterns #-}

module CoinMetrics.EOSArchive
  ( EosArchive(..)
  ) where

import Control.Arrow
import Control.Concurrent
import Control.Concurrent.STM
import Control.Exception
import Control.Monad
import qualified Data.Aeson as J
import Data.Bits
import qualified Data.ByteString as B
import qualified Data.ByteString.Char8 as BC8
import qualified Data.ByteString.Short as BS
import qualified Data.HashMap.Lazy as HM
import Data.Int
import Data.Maybe
import Data.Proxy
import qualified Data.Serialize as S
import qualified Data.Text as T
import qualified Data.Text.Encoding as T
import qualified Data.Text.Encoding.Error as T
import Data.Time.Clock.POSIX
import qualified Data.Vector as V
import Data.Word
import qualified GHC.Generics as G
import qualified Network.HTTP.Client as H
import qualified Network.WebSockets as WS

import CoinMetrics.BlockChain
import CoinMetrics.Schema.Util
import CoinMetrics.Util
import Hanalytics.Schema(schemaOf)

-- | Class for ABI EOS serialization.
class Abi a where
  fromAbiBinary :: S.Get a
  default fromAbiBinary :: (G.Generic a, GenericAbiDatatype (G.Rep a)) => S.Get a
  fromAbiBinary = G.to <$> genericDatatypeFromAbiBinary
  toAbiBinary :: S.Putter a
  default toAbiBinary :: (G.Generic a, GenericAbiDatatype (G.Rep a)) => S.Putter a
  toAbiBinary = genericDatatypeToAbiBinary . G.from

-- read_varuint64
varIntFromAbiBinary :: S.Get Word64
varIntFromAbiBinary = let
  step value offset = do
    b <- S.getWord8
    let
      r = value .|. (fromIntegral (b .&. 0x7f) `shiftL` offset)
    if b `testBit` 7
      then step r (offset + 7)
      else return r
  in step 0 0

  -- push_varuint64
varIntToAbiBinary :: S.Putter Word64
varIntToAbiBinary = let
  step value = do
    let
      newValue = value `shiftR` 7
    S.putWord8 (fromIntegral (value .&. 0x7f) .|. (if newValue > 0 then 0x80 else 0x00))
    when (newValue > 0) $ step newValue
  in step

instance Abi Word8 where
  fromAbiBinary = S.getWord8
  toAbiBinary = S.putWord8

instance Abi Word16 where
  fromAbiBinary = S.getWord16le
  toAbiBinary = S.putWord16le
instance Abi Int16 where
  fromAbiBinary = S.getInt16le
  toAbiBinary = S.putInt16le

instance Abi Word32 where
  fromAbiBinary = S.getWord32le
  toAbiBinary = S.putWord32le
instance Abi Int32 where
  fromAbiBinary = S.getInt32le
  toAbiBinary = S.putInt32le

instance Abi Word64 where
  fromAbiBinary = S.getWord64le
  toAbiBinary = S.putWord64le
instance Abi Int64 where
  fromAbiBinary = S.getInt64le
  toAbiBinary = S.putInt64le

instance Abi Double where
  fromAbiBinary = S.get
  toAbiBinary = S.put

instance Abi Bool where
  fromAbiBinary = (> 0) <$> S.getWord8
  toAbiBinary f = S.putWord8 (if f then 1 else 0)

instance Abi B.ByteString where
  fromAbiBinary = do
    len <- varIntFromAbiBinary
    S.getBytes (fromIntegral len)
  toAbiBinary bytes = do
    varIntToAbiBinary $ fromIntegral $ B.length bytes
    S.putByteString bytes

instance Abi T.Text where
  fromAbiBinary = T.decodeUtf8With T.lenientDecode <$> fromAbiBinary
  toAbiBinary = toAbiBinary . T.encodeUtf8

instance Abi HexString where
  fromAbiBinary = HexString . BS.toShort <$> fromAbiBinary
  toAbiBinary (HexString bytes) = toAbiBinary (BS.fromShort bytes)

instance Abi a => Abi (Maybe a) where
  fromAbiBinary = do
    presented <- fromAbiBinary
    if presented
      then Just <$> fromAbiBinary
      else return Nothing
  toAbiBinary f = do
    toAbiBinary $ isJust f
    mapM_ toAbiBinary f

instance Abi a => Abi (V.Vector a) where
  fromAbiBinary = do
    len <- fromIntegral <$> varIntFromAbiBinary
    V.replicateM len fromAbiBinary
  toAbiBinary v = do
    varIntToAbiBinary (fromIntegral $ V.length v)
    mapM_ toAbiBinary v


-- ABI EOS implementation for Generic types.

class GenericAbiDatatype f where
  genericDatatypeFromAbiBinary :: S.Get (f p)
  genericDatatypeToAbiBinary :: S.Putter (f p)

class GenericAbiConstructor f where
  genericConstructorFromAbiBinary :: S.Get (f p)
  genericConstructorToAbiBinary :: S.Putter (f p)

class GenericAbiSelector f where
  genericSelectorFromAbiBinary :: S.Get (f p)
  genericSelectorToAbiBinary :: S.Putter (f p)

class GenericAbiValue f where
  genericValueFromAbiBinary :: S.Get (f p)
  genericValueToAbiBinary :: S.Putter (f p)

instance (G.Datatype c, GenericAbiConstructor f) => GenericAbiDatatype (G.M1 G.D c f) where
  genericDatatypeFromAbiBinary = G.M1 <$> genericConstructorFromAbiBinary
  genericDatatypeToAbiBinary = genericConstructorToAbiBinary . G.unM1

instance (G.Constructor c, GenericAbiSelector f) => GenericAbiConstructor (G.M1 G.C c f) where
  genericConstructorFromAbiBinary = G.M1 <$> genericSelectorFromAbiBinary
  genericConstructorToAbiBinary = genericSelectorToAbiBinary . G.unM1

instance (G.Selector c, GenericAbiValue f) => GenericAbiSelector (G.M1 G.S c f) where
  genericSelectorFromAbiBinary = G.M1 <$> genericValueFromAbiBinary
  genericSelectorToAbiBinary = genericValueToAbiBinary . G.unM1
instance GenericAbiSelector G.U1 where
  genericSelectorFromAbiBinary = return G.U1
  genericSelectorToAbiBinary G.U1 = return ()
instance (GenericAbiSelector a, GenericAbiSelector b) => GenericAbiSelector (a G.:*: b) where
  genericSelectorFromAbiBinary = liftM2 (G.:*:) genericSelectorFromAbiBinary genericSelectorFromAbiBinary
  genericSelectorToAbiBinary (a G.:*: b) = genericSelectorToAbiBinary a >> genericSelectorToAbiBinary b

instance Abi a => GenericAbiValue (G.K1 G.R a) where
  genericValueFromAbiBinary = G.K1 <$> fromAbiBinary
  genericValueToAbiBinary = toAbiBinary . G.unK1


-- schema structs

data Schema = Schema
  { schema_typeLoaders :: !(HM.HashMap T.Text (S.Get J.Value))
  }

-- | Init schema from JSON structures.
initSchema :: SchemaInit -> Schema
initSchema SchemaInit
  { schemaInit_structs = structsInit
  , schemaInit_types = typesInit
  , schemaInit_variants = variantsInit
  } = Schema
  { schema_typeLoaders = allLoaders
  } where
  allLoaders = HM.fromList $ addAuxLoaders $ builtinLoaders ++ structsLoaders ++ typesLoaders ++ variantsLoaders

  getLoader typeName = fromMaybe (fail $ "unknown type " <> T.unpack typeName) $ HM.lookup typeName allLoaders

  addAuxLoaders loaders = loaders ++ map optionalLoader loaders ++ map arrayLoader loaders
  optionalLoader (name, valueLoader) = let
    loader = do
      presented <- fromAbiBinary
      if presented
        then valueLoader
        else return J.Null
    in (name <> "?", loader)
  arrayLoader (name, elementLoader) = let
    loader = do
      len <- varIntFromAbiBinary
      J.Array <$> V.replicateM (fromIntegral len) elementLoader
    in (name <> "[]", loader)

  builtinLoaders =
    [ ("uint8", J.toJSON <$> (fromAbiBinary :: S.Get Word8))
    , ("uint16", J.toJSON <$> (fromAbiBinary :: S.Get Word16))
    , ("int16", J.toJSON <$> (fromAbiBinary :: S.Get Int16))
    , ("uint32", J.toJSON <$> (fromAbiBinary :: S.Get Word32))
    , ("int32", J.toJSON <$> (fromAbiBinary :: S.Get Int32))
    , ("uint64", J.toJSON <$> (fromAbiBinary :: S.Get Word64))
    , ("int64", J.toJSON <$> (fromAbiBinary :: S.Get Int64))
    , ("uint128", J.toJSON <$> (fromAbiBinary :: S.Get Word128))
    , ("float64", J.toJSON <$> (fromAbiBinary :: S.Get Double))
    , ("float128", J.toJSON <$> (fromAbiBinary :: S.Get Float128))
    , ("bool", J.toJSON <$> (fromAbiBinary :: S.Get Bool))
    , ("varuint32", J.toJSON <$> varIntFromAbiBinary)
    , ("bytes", J.toJSON <$> (fromAbiBinary :: S.Get HexString))
    , ("string", J.toJSON <$> (fromAbiBinary :: S.Get T.Text))
    , ("name", J.toJSON <$> (fromAbiBinary :: S.Get Name))
    , ("checksum256", J.toJSON <$> (fromAbiBinary :: S.Get Checksum256))
    , ("block_position", J.toJSON <$> (fromAbiBinary :: S.Get BlockPosition))
    , ("block_timestamp_type", J.toJSON <$> (fromAbiBinary :: S.Get Word32))
    , ("time_point", J.toJSON <$> (fromAbiBinary :: S.Get Word64))
    , ("time_point_sec", J.toJSON <$> (fromAbiBinary :: S.Get Word32))
    , ("public_key", J.toJSON <$> (fromAbiBinary :: S.Get PublicKey))
    , ("signature", J.toJSON <$> (fromAbiBinary :: S.Get Signature))
    ]

  structsLoaders = flip map (V.toList structsInit) $ \SchemaStructInit
    { schemaStructInit_name = name
    , schemaStructInit_base = maybeBase
    , schemaStructInit_fields = fields
    } -> let
    baseLoader = maybe (return J.Null) getLoader maybeBase
    fieldLoaders = V.map (schemaField_name &&& (getLoader . schemaField_type)) fields
    loader = do
      baseObject <- baseLoader
      let
        baseFields = case baseObject of
          J.Object f -> f
          _ -> HM.empty
      ownFields <- forM fieldLoaders $ \(fieldName, fieldLoader) -> do
        fieldValue <- fieldLoader
        return (fieldName, fieldValue)
      return $ J.Object $ HM.union baseFields (HM.fromList $ V.toList ownFields)
    in (name, loader)

  typesLoaders = flip map (V.toList typesInit) $ \SchemaTypeInit
    { schemaTypeInit_new_type_name = name
    , schemaTypeInit_type = t
    } -> (name, getLoader t)

  variantsLoaders = flip map (V.toList variantsInit) $ \SchemaVariantInit
    { schemaVariantInit_name = name
    , schemaVariantInit_types = types
    } -> let
    variantLoaders = V.map getLoader types
    loader = do
      variantIndex <- varIntFromAbiBinary
      fromMaybe (fail $ "wrong variant index: " <> T.unpack name <> ", " <> show variantIndex) $ variantLoaders V.!? (fromIntegral variantIndex)
    in (name, loader)

-- | Read dynamic EOSABI type into JSON using schema.
dynamicFromAbiBinary :: Schema -> T.Text -> S.Get J.Value
dynamicFromAbiBinary Schema
  { schema_typeLoaders = typeLoaders
  } typeName = fromMaybe (fail $ "unknown dynamic type " <> T.unpack typeName) $ HM.lookup typeName typeLoaders

-- | Perform additional decoding of rows.
decodeTableRowsAbi :: Schema -> J.Value -> J.Value
decodeTableRowsAbi schema table = case table of
  J.Object tableFields@(HM.lookup "name" &&& HM.lookup "rows" -> (Just (J.String rowType), Just (J.Array rows))) -> let
    rowLoader = dynamicFromAbiBinary schema rowType
    parseRowData = S.runGet $ do
      r <- rowLoader
      isEmpty <- S.isEmpty
      unless isEmpty $ fail "not all row input consumed"
      return r
    newRows = flip V.map rows $ \rowValue -> case rowValue of
      J.Object rowFields@(HM.lookup "data" -> Just (J.fromJSON -> J.Success (HexString (either error id . parseRowData . BS.fromShort -> rowData)))) ->
        J.Object $ HM.insert "data" rowData rowFields
      _ -> rowValue
    in J.Object $ HM.insert "rows" (J.Array newRows) tableFields
  _ -> table

-- JSON structs for reading schema

data SchemaInit = SchemaInit
  { schemaInit_version :: !T.Text
  , schemaInit_structs :: !(V.Vector SchemaStructInit)
  , schemaInit_types :: !(V.Vector SchemaTypeInit)
  , schemaInit_variants :: !(V.Vector SchemaVariantInit)
  , schemaInit_tables :: !(V.Vector SchemaTableInit)
  } deriving G.Generic
instance J.FromJSON SchemaInit where
  parseJSON = J.genericParseJSON schemaJsonOptions

data SchemaStructInit = SchemaStructInit
  { schemaStructInit_name :: !T.Text
  , schemaStructInit_base :: !(Maybe T.Text)
  , schemaStructInit_fields :: !(V.Vector SchemaField)
  } deriving G.Generic
instance J.FromJSON SchemaStructInit where
  parseJSON = J.genericParseJSON schemaJsonOptions

data SchemaTypeInit = SchemaTypeInit
  { schemaTypeInit_new_type_name :: !T.Text
  , schemaTypeInit_type :: !T.Text
  } deriving G.Generic
instance J.FromJSON SchemaTypeInit where
  parseJSON = J.genericParseJSON schemaJsonOptions

data SchemaVariantInit = SchemaVariantInit
  { schemaVariantInit_name :: !T.Text
  , schemaVariantInit_types :: !(V.Vector T.Text)
  } deriving G.Generic
instance J.FromJSON SchemaVariantInit where
  parseJSON = J.genericParseJSON schemaJsonOptions

data SchemaTableInit = SchemaTableInit
  { schemaTableInit_name :: !T.Text
  , schemaTableInit_type :: !T.Text
  , schemaTableInit_key_names :: !(V.Vector T.Text)
  } deriving G.Generic
instance J.FromJSON SchemaTableInit where
  parseJSON = J.genericParseJSON schemaJsonOptions

data SchemaField = SchemaField
  { schemaField_name :: !T.Text
  , schemaField_type :: !T.Text
  } deriving G.Generic
instance J.FromJSON SchemaField where
  parseJSON = J.genericParseJSON schemaJsonOptions

data EosArchive = EosArchive
  { ea_httpRequest :: !H.Request
  , ea_clientsVar :: {-# UNPACK #-} !(TVar [EosArchiveClient])
  }

data EosArchiveClient = EosArchiveClient
  { eac_connection :: !WS.Connection
  , eac_schema :: !Schema
  , eac_statusResultVarVar :: {-# UNPACK #-} !(TVar (Maybe (TVar (Maybe StatusResult))))
  , eac_blockResultsVarsVar :: {-# UNPACK #-} !(TVar (HM.HashMap BlockHeight (TVar (Maybe BlocksResult))))
  , eac_exceptionVar :: {-# UNPACK #-} !(TVar (Maybe SomeException))
  }

data EosArchiveBlock = EosArchiveBlock
  { eab_number :: {-# UNPACK #-} !Int64
  , eab_block :: !J.Value
  , eab_traces :: !J.Value
  , eab_deltas :: !J.Value
  }

instance HasBlockHeader EosArchiveBlock where
  getBlockHeader EosArchiveBlock
    { eab_number = number
    } = BlockHeader
    { bh_height = number
    , bh_hash = mempty
    , bh_prevHash = Nothing
    , bh_timestamp = posixSecondsToUTCTime 0
    }

data StatusRequest = StatusRequest -- get_status_request_v0
  deriving G.Generic
instance Abi StatusRequest

data BlocksRequest = BlocksRequest -- get_blocks_request_v0
  { req_start_block_num :: {-# UNPACK #-} !Word32
  , req_end_block_num :: {-# UNPACK #-} !Word32
  , req_max_messages_in_flight :: {-# UNPACK #-} !Word32
  , req_have_positions :: !(V.Vector BlockPosition)
  , req_irreversible_only :: !Bool
  , req_fetch_block :: !Bool
  , req_fetch_traces :: !Bool
  , req_fetch_deltas :: !Bool
  }
  deriving G.Generic
instance Abi BlocksRequest

data Request
  = Request_status StatusRequest
  | Request_blocks BlocksRequest
instance Abi Request where
  fromAbiBinary = do
    variantIndex <- varIntFromAbiBinary
    case variantIndex of
      0 -> Request_status <$> fromAbiBinary
      1 -> Request_blocks <$> fromAbiBinary
      _ -> fail "wrong request"
  toAbiBinary = \case
    Request_status a -> do
      varIntToAbiBinary 0
      toAbiBinary a
    Request_blocks a -> do
      varIntToAbiBinary 1
      toAbiBinary a

data StatusResult = StatusResult -- get_status_result_v0
  { sres_head :: !BlockPosition
  , sres_last_irreversible :: !BlockPosition
  , sres_trace_begin_block :: {-# UNPACK #-} !Word32
  , sres_trace_end_block :: {-# UNPACK #-} !Word32
  , sres_chain_state_begin_block :: {-# UNPACK #-} !Word32
  , sres_chain_state_end_block :: {-# UNPACK #-} !Word32
  }
  deriving G.Generic
instance Abi StatusResult

data BlocksResult = BlocksResult -- get_blocks_result_v0
  { bres_head :: !BlockPosition
  , bres_last_irreversible :: !BlockPosition
  , bres_this_block :: !(Maybe BlockPosition)
  , bres_prev_block :: !(Maybe BlockPosition)
  , bres_block :: !(Maybe HexString)
  , bres_traces :: !(Maybe HexString)
  , bres_deltas :: !(Maybe HexString)
  }
  deriving G.Generic
instance Abi BlocksResult

data Result
  = Result_status StatusResult
  | Result_blocks BlocksResult
instance Abi Result where
  fromAbiBinary = do
    variantIndex <- varIntFromAbiBinary
    case variantIndex of
      0 -> Result_status <$> fromAbiBinary
      1 -> Result_blocks <$> fromAbiBinary
      _ -> fail "wrong result"
  toAbiBinary = \case
    Result_status a -> do
      varIntToAbiBinary 0
      toAbiBinary a
    Result_blocks a -> do
      varIntToAbiBinary 1
      toAbiBinary a

data BlockPosition = BlockPosition
  { bp_block_num :: {-# UNPACK #-} !Word32
  , bp_block_id :: !Checksum256
  } deriving G.Generic
instance J.ToJSON BlockPosition where
  toJSON = J.genericToJSON schemaJsonOptions
  toEncoding = J.genericToEncoding schemaJsonOptions
instance Abi BlockPosition

newtype Name = Name Word64 deriving Abi
instance J.ToJSON Name where
  toJSON = J.toJSON . nameToString
  toEncoding = J.toEncoding . nameToString
nameToString :: Name -> T.Text
nameToString (Name name) = step (0 :: Int) name "" where
  step i n s = if i <= 12
    then step (i + 1) (n `shiftR` (if i == 0 then 4 else 5)) (T.index alphabet (fromIntegral $ n .&. (if i == 0 then 0x0f else 0x1f)) : s)
    else T.dropWhileEnd (== '.') (T.pack s)
  alphabet = ".12345abcdefghijklmnopqrstuvwxyz"

newtype Word128 = Word128 Integer deriving (J.FromJSON, J.ToJSON)
instance Abi Word128 where
  fromAbiBinary = do
    a <- S.getWord64le
    b <- S.getWord64le
    return $ Word128 $ fromIntegral a .|. (fromIntegral b `shiftL` 64)
  toAbiBinary (Word128 n) = do
    S.putWord64le $ fromIntegral (n .&. 0xFFFFFFFF)
    S.putWord64le $ fromIntegral (n `shiftR` 64)

newtype Float128 = Float128 HexString deriving (J.FromJSON, J.ToJSON)
instance Abi Float128 where
  fromAbiBinary = Float128 . HexString . BS.toShort <$> S.getBytes 16
  toAbiBinary (Float128 (HexString bytes)) = S.putByteString $ BS.fromShort bytes

newtype Checksum256 = Checksum256 HexString deriving (J.FromJSON, J.ToJSON)
instance Abi Checksum256 where
  fromAbiBinary = Checksum256 . HexString . BS.toShort <$> S.getBytes 32
  toAbiBinary (Checksum256 (HexString bytes)) = S.putByteString $ BS.fromShort bytes

data PublicKey = PublicKey
  { pubk_type :: {-# UNPACK #-} !Word8
  , pubk_data :: !HexString
  } deriving G.Generic
instance J.ToJSON PublicKey where
  toJSON = J.genericToJSON schemaJsonOptions
  toEncoding = J.genericToEncoding schemaJsonOptions
instance Abi PublicKey where
  fromAbiBinary = PublicKey
    <$> fromAbiBinary
    <*> (HexString . BS.toShort <$> S.getBytes 33)
  toAbiBinary (PublicKey t (HexString d)) = do
    toAbiBinary t
    S.putByteString (BS.fromShort d)

data Signature = Signature
  { s_type :: {-# UNPACK #-} !Word8
  , s_data :: !HexString
  } deriving G.Generic
instance J.ToJSON Signature where
  toJSON = J.genericToJSON schemaJsonOptions
  toEncoding = J.genericToEncoding schemaJsonOptions
instance Abi Signature where
  fromAbiBinary = Signature
    <$> fromAbiBinary
    <*> (HexString . BS.toShort <$> S.getBytes 65)
  toAbiBinary (Signature t (HexString d)) = do
    toAbiBinary t
    S.putByteString (BS.fromShort d)

genSchemaInstances [''EosArchiveBlock]

instance BlockChain EosArchive where
  type Block EosArchive = EosArchiveBlock

  getBlockChainInfo _ = BlockChainInfo
    { bci_init = \BlockChainParams
      { bcp_httpRequest = httpRequest
      } -> do
      clientsVar <- newTVarIO []
      return EosArchive
        { ea_httpRequest = httpRequest
        , ea_clientsVar = clientsVar
        }
    , bci_defaultApiUrls = ["http://127.0.0.1:8080/"]
    , bci_defaultBeginBlock = 2
    , bci_defaultEndBlock = 0 -- no need in a gap, as it uses irreversible block number
    , bci_heightFieldName = "number"
    , bci_schemas = standardBlockChainSchemas (schemaOf (Proxy :: Proxy EosArchiveBlock)) []
      "CREATE TABLE \"eos_archive\" OF \"EosArchiveBlock\" (PRIMARY KEY (\"number\"));"
    }

  getCurrentBlockHeight archive = do
    StatusResult
      { sres_last_irreversible = BlockPosition irreversibleBlock _
      , sres_trace_end_block = traceEndBlock
      , sres_chain_state_end_block = chainStateEndBlock
      } <- withEosArchiveClient archive requestEosArchiveStatus
    return $ fromIntegral $ min irreversibleBlock $ min traceEndBlock chainStateEndBlock

  getBlockByHeight archive blockHeight = do
    (BlocksResult
      { bres_this_block = Just (BlockPosition ((== blockHeight) . fromIntegral -> True) _)
      , bres_block = Just (HexString (BS.fromShort -> blockBytes))
      , bres_traces = Just (HexString (BS.fromShort -> tracesBytes))
      , bres_deltas = Just (HexString (BS.fromShort -> deltasBytes))
      }, schema) <- withEosArchiveClient archive $ \archiveClient@EosArchiveClient
      { eac_schema = schema
      } -> (, schema) <$> requestEosArchiveBlock archiveClient blockHeight
    block <- either fail return $ flip S.runGet blockBytes $ do
      r <- dynamicFromAbiBinary schema "signed_block"
      isEmpty <- S.isEmpty
      unless isEmpty $ fail "not all block input consumed"
      return r
    traces <- either fail return $ flip S.runGet tracesBytes $ do
      r <- dynamicFromAbiBinary schema "transaction_trace[]"
      isEmpty <- S.isEmpty
      unless isEmpty $ fail "not all traces input consumed"
      return r
    deltas <- either fail return $ flip S.runGet deltasBytes $ do
      J.Array tables <- dynamicFromAbiBinary schema "table_delta[]"
      let
        r = J.Array $ V.map (decodeTableRowsAbi schema) tables
      isEmpty <- S.isEmpty
      unless isEmpty $ fail "not all deltas input consumed"
      return r
    return EosArchiveBlock
      { eab_number = blockHeight
      , eab_block = block
      , eab_traces = traces
      , eab_deltas = deltas
      }

startEosArchiveClient :: H.Request -> IO EosArchiveClient
startEosArchiveClient httpRequest = do
  initVar <- newTVarIO Nothing
  exceptionVar <- newTVarIO Nothing
  timedOutVar <- registerDelay networkTimeout
  statusResultVarVar <- newTVarIO Nothing
  blockResultsVarsVar <- newTVarIO HM.empty

  -- start thread
  void $ forkIO $ handle (atomically . writeTVar exceptionVar . Just) $
    WS.runClient (BC8.unpack $ H.host httpRequest) (H.port httpRequest) (BC8.unpack $ H.path httpRequest) $ \connection -> do
      -- receive first message (schema)
      schema <- either fail (return . initSchema) . J.eitherDecode =<< WS.receiveData connection
      -- signal initialization
      atomically $ writeTVar initVar $ Just (connection, schema)
      -- receive following messages in a loop
      forever $ do
        message <- WS.receiveData connection
        result <- either fail return $ flip S.runGet message $ do
          r <- fromAbiBinary
          isEmpty <- S.isEmpty
          unless isEmpty $ fail "not all result input consumed"
          return r
        case result of
          Result_status statusResult -> atomically $ do
            maybeStatusResultVar <- readTVar statusResultVarVar
            case maybeStatusResultVar of
              Just statusResultVar -> do
                writeTVar statusResultVar $ Just statusResult
                writeTVar statusResultVarVar Nothing
              Nothing -> return ()
          Result_blocks blockResult@BlocksResult
            { bres_this_block = Just (BlockPosition (fromIntegral -> blockHeight) _)
            } -> atomically $ do
            blockResultsVars <- readTVar blockResultsVarsVar
            case HM.lookup blockHeight blockResultsVars of
              Just blockResultVar -> do
                writeTVar blockResultVar $ Just blockResult
                writeTVar blockResultsVarsVar $ HM.delete blockHeight blockResultsVars
              Nothing -> return ()
          _ -> return ()

  -- wait for initialization
  (connection, schema) <- atomically $ do
    mapM_ throwSTM =<< readTVar exceptionVar
    timedOut <- readTVar timedOutVar
    when timedOut $ throwSTM EosArchiveTimeout
    maybe retry return =<< readTVar initVar

  return EosArchiveClient
    { eac_connection = connection
    , eac_schema = schema
    , eac_statusResultVarVar = statusResultVarVar
    , eac_blockResultsVarsVar = blockResultsVarsVar
    , eac_exceptionVar = exceptionVar
    }

withEosArchiveClient :: EosArchive -> (EosArchiveClient -> IO a) -> IO a
withEosArchiveClient EosArchive
  { ea_httpRequest = httpRequest
  , ea_clientsVar = clientsVar
  } action = do
  -- get existing client if possible
  maybeClient <- atomically $ do
    clients <- readTVar clientsVar
    case clients of
      client : restClients -> do
        writeTVar clientsVar restClients
        return $ Just client
      [] -> return Nothing
  -- create new client if needed
  client@EosArchiveClient
    { eac_connection = connection
    } <- maybe (startEosArchiveClient httpRequest) return maybeClient
  -- run action
  eitherResult <- try $ action client
  case eitherResult of
    Right result -> do
      -- if we are here, there was no error and we can return client to the pool
      atomically $ modifyTVar clientsVar (client :)
      return result
    Left (SomeException err) -> do
      -- forcibly stop connection
      void (try $ WS.sendClose connection B.empty :: IO (Either SomeException ()))
      throwIO err

requestEosArchiveStatus :: EosArchiveClient -> IO StatusResult
requestEosArchiveStatus EosArchiveClient
  { eac_connection = connection
  , eac_statusResultVarVar = statusResultVarVar
  , eac_exceptionVar = exceptionVar
  } = do
  -- check that status has not been requested already
  (statusResultVar, needRequest) <- atomically $ do
    maybeStatusResultVar <- readTVar statusResultVarVar
    case maybeStatusResultVar of
      Just statusResultVar -> return (statusResultVar, False)
      Nothing -> do
        statusResultVar <- newTVar Nothing
        writeTVar statusResultVarVar $ Just statusResultVar
        return (statusResultVar, True)

  -- send request if needed
  when needRequest $ WS.sendBinaryData connection $ S.runPut $ toAbiBinary $ Request_status StatusRequest

  -- wait for result
  timedOutVar <- registerDelay networkTimeout
  atomically $ do
    mapM_ throwSTM =<< readTVar (exceptionVar :: TVar (Maybe SomeException))
    timedOut <- readTVar timedOutVar
    when timedOut $ throwSTM EosArchiveTimeout
    maybe retry return =<< readTVar statusResultVar

requestEosArchiveBlock :: EosArchiveClient -> BlockHeight -> IO BlocksResult
requestEosArchiveBlock EosArchiveClient
  { eac_connection = connection
  , eac_blockResultsVarsVar = blockResultsVarsVar
  , eac_exceptionVar = exceptionVar
  } blockHeight = do
  -- check that block has not been requested already
  (blockResultVar, needRequest) <- atomically $ do
    blockResultsVars <- readTVar blockResultsVarsVar
    case HM.lookup blockHeight blockResultsVars of
      Just blockResultVar -> return (blockResultVar, False)
      Nothing -> do
        blockResultVar <- newTVar Nothing
        writeTVar blockResultsVarsVar $ HM.insert blockHeight blockResultVar blockResultsVars
        return (blockResultVar, True)

  -- send request if needed
  when needRequest $ WS.sendBinaryData connection $ S.runPut $ toAbiBinary $ Request_blocks BlocksRequest
    { req_start_block_num = fromIntegral blockHeight
    , req_end_block_num = fromIntegral blockHeight + 1
    , req_max_messages_in_flight = maxBound
    , req_have_positions = V.empty
    , req_irreversible_only = True
    , req_fetch_block = True
    , req_fetch_traces = True
    , req_fetch_deltas = True
    }

  -- wait for result
  timedOutVar <- registerDelay networkTimeout
  atomically $ do
    mapM_ throwSTM =<< readTVar (exceptionVar :: TVar (Maybe SomeException))
    timedOut <- readTVar timedOutVar
    when timedOut $ throwSTM EosArchiveTimeout
    maybe retry return =<< readTVar blockResultVar

-- | Just an arbitrary timeout (in microseconds).
networkTimeout :: Int
networkTimeout = 30000000

data EosArchiveException
  = EosArchiveTimeout
  deriving Show
instance Exception EosArchiveException
