{-# LANGUAGE OverloadedStrings #-}

module Main (
    main
    ) where

import Prelude hiding (log)

import Control.Exception (SomeException)
import Control.Monad
import Control.Monad.IO.Class
import Control.Monad.CatchIO
import Data.ByteString (ByteString)
import qualified Data.Text as T
import qualified Data.Text.Encoding as T
import qualified Data.Map as M
import Data.Maybe
import Data.String
import Data.List hiding (insert)
import System.Environment
import System.Log
import Text.Read

import qualified Database.Redis as R
import qualified Database.PostgreSQL.Simple as P

import Carma.ModelTables

-- convert arguments to map
-- ["-f1", "value1", "-f2", "value2"] => fromList [("-f1", "value1"), ("-f2", "value2")]
arguments :: [String] -> M.Map String String
arguments = M.fromList . mapMaybe toTuple . splitBy 2 where
  toTuple :: [a] -> Maybe (a, a)
  toTuple [x, y] = Just (x, y)
  toTuple _ = Nothing

  splitBy :: Int -> [a] -> [[a]]
  splitBy = unfoldr . takedrop

  takedrop :: Int -> [a] -> Maybe ([a], [a])
  takedrop _ [] = Nothing
  takedrop n xs = Just (take n xs, drop n xs)

-- convert arguments to map with replacing default values
args :: [(String, String)] -> [String] -> M.Map String String
args as s = arguments s `M.union` M.fromList as

rules :: String -> Rules
rules r = [
  parseRule_ (fromString $ "/: use " ++ r),
  parseRule_ (fromString $ "insertUpdate: set fatal fatal"),
  parseRule_ (fromString $ "insert: set fatal fatal"),
  parseRule_ (fromString $ "update: set fatal fatal"),
  parseRule_ (fromString $ "createExtend: set fatal fatal")]

usage :: IO ()
usage = mapM_ putStrLn [
    "Usage: carma-sync [flags] where",
    "  -m <model> - model to sync, all by default",
    "  -f <from-id> - id to sync from",
    "  -t <to-id> - id to sync to",
    "  -l <level> - log level, default is 'default', possible values are: trace, debug, default, silent",
    "",
    "Examples:",
    "  carma-sync -m partner",
    "  carma-sync -m case -f 10000 -t 12000 -l trace"]

pconinfo :: P.ConnectInfo
pconinfo = P.defaultConnectInfo {
    P.connectUser = "carma_db_sync",
    P.connectDatabase = "carma",
    P.connectPassword = "pass" }

argsDecl :: [(String, String)]
argsDecl = [
    ("-m", ""),
    ("-f", ""),
    ("-t", ""),
    ("-l", "default")]

main :: IO ()
main = do
    as <- getArgs
    if as == ["--help"]
        then usage
        else main' (args argsDecl as)
    where
        main' flags = do
            l <- newLog (constant (rules $ flag "-l"))
                [logger text (file "log/carma-sync.log")]
            withLog l $ scope "main" $ do
                descs <- liftIO $ loadTables
                    "resources/site-config/models"
                    "resources/site-config/field-groups.json"
                descs' <- case (flag "-m") of
                    "" -> return descs
                    mdl' -> case find ((== mdl') . tableModel) descs of
                        Nothing -> error "Unknown model"
                        Just desc' -> return [desc']
                rcon <- liftIO $ R.connect R.defaultConnectInfo
                pcon <- liftIO $ P.connect pconinfo
                let
                    sync' :: MonadLog m => TableDesc -> m ()
                    sync' d = catch (sync rcon pcon (readMaybe $ flag "-f") (readMaybe $ flag "-t") d) onError where
                        onError :: MonadIO m => SomeException -> m ()
                        onError = liftIO . putStrLn . show
                mapM_ sync' descs'
            where
                flag = (flags M.!)

str :: IsString s => Int -> s
str = fromString . show

-- | Sync model
sync :: MonadLog m => R.Connection -> P.Connection -> Maybe Int -> Maybe Int -> TableDesc -> m ()
sync rcon pcon from to desc = scope "sync" $ scope (fromString $ tableModel desc) $ do
    log Info $ fromString $ "Syncing " ++ tableModel desc
    createExtend pcon desc
    let
        from' = fromMaybe 1 from
        getMaxId = liftIO $ R.runRedis rcon $ do
            (Right (Just maxId)) <- R.get $ fromString ("global:" ++ tableModel desc ++ ":id")
            either error return $ readEither $ T.unpack $ T.decodeUtf8 maxId
    to' <- maybe getMaxId return to
    log Debug $ fromString $ "Syncing from " ++ show from' ++ " to " ++ show to'

    -- get all id between from' and to'
    -- update existing values and insert new values
    ids <- liftIO $ liftM (map P.fromOnly) $ P.query pcon (fromString $ "select id from " ++ tableName desc ++ " where (id >= ?) and (id <= ?)") (from', to')

    log Debug $ fromString $ "Updating " ++ show (length ids) ++ " rows"
    forM_ ids $ \i -> ignoreError $ scope (str i) $ do
        (Right v) <- liftIO $ R.runRedis rcon $ R.hgetall $ fromString $ tableModel desc ++ ":" ++ show i
        when (not $ null v) $ update pcon desc (str i) (M.fromList v)

    log Debug $ fromString $ "Inserting " ++ show (to' + 1 - from' - length ids) ++ " new rows"
    forM_ ([from'..to'] \\ ids) $ \i -> ignoreError $ scope (str i) $ do
        (Right v) <- liftIO $ R.runRedis rcon $ R.hgetall $ fromString $ tableModel desc ++ ":" ++ show i
        when (not $ null v) $ insert pcon desc (M.insert "id" (str i) $ M.fromList v)
