{-# LANGUAGE DeriveGeneric, LambdaCase, OverloadedLists, OverloadedStrings, StandaloneDeriving, TemplateHaskell #-}

module CoinMetrics.Iota
  ( IotaTransaction(..)
  , Iota(..)
  , newIota
  , iotaGetTips
  , iotaGetMilestones
  , iotaGetTransactions
  , deserIotaTransaction
  ) where

import Control.Monad
import qualified Data.Aeson as J
import qualified Data.Aeson.Types as J
import Data.Either
import Data.Int
import qualified Data.Serialize as S
import qualified Data.Text as T
import qualified Data.Text.Encoding as T
import qualified Data.Vector as V
import qualified Network.HTTP.Client as H

import CoinMetrics.Schema.Util
import CoinMetrics.Util

data IotaTransaction = IotaTransaction
  { it_hash :: !T.Text
  , it_signature :: !T.Text
  , it_address :: !T.Text
  , it_value :: !Int64
  , it_obsoleteTag :: !T.Text
  , it_timestamp :: !Int64
  , it_currentIndex :: !Int64
  , it_lastIndex :: !Int64
  , it_bundle :: !T.Text
  , it_trunkTransaction :: !T.Text
  , it_branchTransaction :: !T.Text
  , it_tag :: !T.Text
  , it_attachmentTimestamp :: !Int64
  , it_attachmentTimestampLowerBound :: !Int64
  , it_attachmentTimestampUpperBound :: !Int64
  , it_nonce :: !T.Text
  }

genSchemaInstances [''IotaTransaction]

-- | Deserialize transaction.
deserIotaTransaction :: T.Text -> S.Get IotaTransaction
deserIotaTransaction hash = IotaTransaction hash
  <$> (T.decodeUtf8 <$> S.getByteString 2187)
  <*> (T.decodeUtf8 <$> S.getByteString 81)
  <*> deserInt 11
  <*> (T.decodeUtf8 <$> (check16ZeroTrytes >> S.getByteString 27))
  <*> deserInt 9
  <*> deserInt 9
  <*> deserInt 9
  <*> (T.decodeUtf8 <$> S.getByteString 81)
  <*> (T.decodeUtf8 <$> S.getByteString 81)
  <*> (T.decodeUtf8 <$> S.getByteString 81)
  <*> (T.decodeUtf8 <$> S.getByteString 27)
  <*> deserInt 9
  <*> deserInt 9
  <*> deserInt 9
  <*> (T.decodeUtf8 <$> S.getByteString 27)

-- | Deserialize 27-trit (9-tryte) integer.
deserInt :: Int -> S.Get Int64
deserInt n = f 0 0 1 where
  f i s p = if i >= n then return s else do
    t <- deserTryte
    f (i + 1) (s + t * p) (p * 27)

-- | Check that there's 16 zero trytes.
check16ZeroTrytes :: S.Get ()
check16ZeroTrytes = do
  trytes <- S.getByteString 16
  unless (trytes == "9999999999999999") $ fail "wrong zero trytes"

-- | Deserialize a tryte.
deserTryte :: S.Get Int64
deserTryte = f <$> S.getWord8 where
  f = \case
    57 {- '9' -} -> 0
    65 {- 'A' -} -> 1
    66 {- 'B' -} -> 2
    67 {- 'C' -} -> 3
    68 {- 'D' -} -> 4
    69 {- 'E' -} -> 5
    70 {- 'F' -} -> 6
    71 {- 'G' -} -> 7
    72 {- 'H' -} -> 8
    73 {- 'I' -} -> 9
    74 {- 'J' -} -> 10
    75 {- 'K' -} -> 11
    76 {- 'L' -} -> 12
    77 {- 'M' -} -> 13
    78 {- 'N' -} -> -13
    79 {- 'O' -} -> -12
    80 {- 'P' -} -> -11
    81 {- 'Q' -} -> -10
    82 {- 'R' -} -> -9
    83 {- 'S' -} -> -8
    84 {- 'T' -} -> -7
    85 {- 'U' -} -> -6
    86 {- 'V' -} -> -5
    87 {- 'W' -} -> -4
    88 {- 'X' -} -> -3
    89 {- 'Y' -} -> -2
    90 {- 'Z' -} -> -1
    _ -> error "wrong tryte"

-- | IOTA connector.
data Iota = Iota
  { iota_httpManager :: !H.Manager
  , iota_httpRequest :: !H.Request
  }

newIota :: H.Manager -> H.Request -> Iota
newIota httpManager httpRequest = Iota
  { iota_httpManager = httpManager
  , iota_httpRequest = httpRequest
    { H.method = "POST"
    , H.requestHeaders =
      [ ("Content-Type", "application/json")
      , ("X-IOTA-API-Version", "1")
      ]
    }
  }

iotaRequest :: J.FromJSON a => Iota -> (a -> J.Parser r) -> J.Value -> IO r
iotaRequest Iota
  { iota_httpManager = httpManager
  , iota_httpRequest = httpRequest
  } parser value = do
  body <- H.responseBody <$> tryWithRepeat (H.httpLbs httpRequest
    { H.requestBody = H.RequestBodyLBS $ J.encode value
    } httpManager)
  result <- either fail return $ J.eitherDecode body
  either fail return $ J.parseEither parser result

iotaGetTips :: Iota -> IO (V.Vector T.Text)
iotaGetTips iota = iotaRequest iota (J..: "hashes") $ J.Object
  [ ("command", "getTips")
  ]

iotaGetMilestones :: Iota -> IO (V.Vector T.Text)
iotaGetMilestones iota = iotaRequest iota parseMilestones $ J.Object
  [ ("command", "getNodeInfo")
  ] where
  parseMilestones o = do
    milestone <- o J..: "latestMilestone"
    solidMilestone <- o J..: "latestSolidSubtangleMilestone"
    return $ V.fromList [milestone, solidMilestone]

iotaGetTransactions :: Iota -> V.Vector T.Text -> IO (V.Vector IotaTransaction)
iotaGetTransactions iota hashes = f <$> iotaRequest iota (J..: "trytes") (J.Object
  [ ("command", "getTrytes")
  , ("hashes", J.toJSON hashes)
  ])
  where f = V.zipWith (\hash trytes -> fromRight (error "wrong transaction") $ S.runGet (deserIotaTransaction hash) $ T.encodeUtf8 trytes) hashes
