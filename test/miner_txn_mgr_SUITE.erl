-module(miner_txn_mgr_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("kernel/include/inet.hrl").
-include_lib("blockchain/include/blockchain_vars.hrl").
-include("miner_ct_macros.hrl").

-export([
         init_per_suite/1,
         end_per_suite/1,
         init_per_testcase/2,
         end_per_testcase/2,
         all/0
        ]).

-export([
         txn_in_sequence_nonce_test/1,
         txn_out_of_sequence_nonce_test/1,
         txn_invalid_nonce_test/1,
         txn_dependent_test/1,
         txn_assert_loc_v2_test/1

        ]).

%% common test callbacks

all() -> [
          txn_in_sequence_nonce_test,
          txn_out_of_sequence_nonce_test,
          txn_invalid_nonce_test,
          txn_dependent_test,
         txn_assert_loc_v2_test
         ].

init_per_suite(Config) ->
    Config.

end_per_suite(Config) ->
    Config.

init_per_testcase(_TestCase, Config0) ->
    Config = miner_ct_utils:init_per_testcase(?MODULE, _TestCase, Config0),
    try
    Miners = ?config(miners, Config),
    Addresses = ?config(addresses, Config),
    InitialPaymentTransactions = [ blockchain_txn_coinbase_v1:new(Addr, 5000) || Addr <- Addresses],
    CoinbaseDCTxns = [blockchain_txn_dc_coinbase_v1:new(Addr, 50000000) || Addr <- Addresses],
    AddGwTxns = [blockchain_txn_gen_gateway_v1:new(Addr, Addr, h3:from_geo({37.780586, -122.469470}, 13), 0)
                 || Addr <- Addresses],

    NumConsensusMembers = ?config(num_consensus_members, Config),
    BlockTime =
        case _TestCase of
            txn_dependent_test -> 5000;
            txn_assert_loc_v2_test -> 5000;
            _ -> ?config(block_time, Config)
        end,

    BatchSize = ?config(batch_size, Config),
    Curve = ?config(dkg_curve, Config),

    Keys = libp2p_crypto:generate_keys(ecc_compact),

    InitialVars = miner_ct_utils:make_vars(Keys, #{?block_time => BlockTime,
                                                   %% rule out rewards
                                                   ?election_interval => infinity,
                                                   ?num_consensus_members => NumConsensusMembers,
                                                   ?batch_size => BatchSize,
                                                   ?dkg_curve => Curve}),

    {ok, DKGCompletionNodes} = miner_ct_utils:initial_dkg(Miners, InitialVars ++ InitialPaymentTransactions ++ CoinbaseDCTxns ++ AddGwTxns,
                                             Addresses, NumConsensusMembers, Curve),
    ct:pal("Nodes which completed the DKG: ~p", [DKGCompletionNodes]),
    %% Get both consensus and non consensus miners
    {ConsensusMiners, NonConsensusMiners} = miner_ct_utils:miners_by_consensus_state(Miners),
    %% integrate genesis block
    _GenesisLoadResults = miner_ct_utils:integrate_genesis_block(hd(DKGCompletionNodes), Miners -- DKGCompletionNodes),
    ct:pal("genesis load results: ~p", [_GenesisLoadResults]),

    %% confirm we have a height of 1
    ok = miner_ct_utils:wait_for_gte(height, Miners, 2),



    [   {consensus_miners, ConsensusMiners},
        {non_consensus_miners, NonConsensusMiners}
        | Config]
    catch
        What:Why ->
            end_per_testcase(_TestCase, Config),
            erlang:What(Why)
    end.


end_per_testcase(_TestCase, Config) ->
    miner_ct_utils:end_per_testcase(_TestCase, Config).


txn_in_sequence_nonce_test(Config) ->
    %% send two standalone payments, with correctly sequenced nonce values
    %% both txns are sent in quick succession
    %% these should clear through the txn mgr right away without probs
    %% and txn mgr cache will clear
    Miner = hd(?config(non_consensus_miners, Config)),
    AddrList = ?config(tagged_miner_addresses, Config),

    ConMiners = ?config(consensus_miners, Config),
    IgnoredTxns = [],
    Addr = miner_ct_utils:node2addr(Miner, AddrList),

    Chain = ct_rpc:call(Miner, blockchain_worker, blockchain, []),

    PayerAddr = Addr,
    Payee = hd(miner_ct_utils:shuffle(ConMiners)),
    PayeeAddr = miner_ct_utils:node2addr(Payee, AddrList),
    {ok, _Pubkey, SigFun, _ECDHFun} = ct_rpc:call(Miner, blockchain_swarm, keys, []),

    StartNonce = miner_ct_utils:get_nonce(Miner, Addr),

    {ok, _Pubkey, SigFun, _ECDHFun} = ct_rpc:call(Miner, blockchain_swarm, keys, []),

    %% the first txn
    Txn1 = ct_rpc:call(Miner, blockchain_txn_payment_v1, new, [PayerAddr, PayeeAddr, 1000, StartNonce+1]),
    SignedTxn1 = ct_rpc:call(Miner, blockchain_txn_payment_v1, sign, [Txn1, SigFun]),
    %% the second txn
    Txn2 = ct_rpc:call(Miner, blockchain_txn_payment_v1, new, [PayerAddr, PayeeAddr, 1000, StartNonce+2]),
    SignedTxn2 = ct_rpc:call(Miner, blockchain_txn_payment_v1, sign, [Txn2, SigFun]),
    %% send the txns
    ok = ct_rpc:call(Miner, blockchain_worker, submit_txn, [SignedTxn1]),
    ok = ct_rpc:call(Miner, blockchain_worker, submit_txn, [SignedTxn2]),

    %% both txns should have been accepted by the CG and removed from the txn mgr cache
    %% txn mgr cache should be empty
    Result = miner_ct_utils:wait_until(
                                        fun()->
                                            case get_cached_txns_with_exclusions(Miner, IgnoredTxns) of
                                                #{} -> true;
                                                _ -> false
                                            end
                                        end, 60, 2000),
    ok = handle_get_cached_txn_result(Result, Miner, IgnoredTxns, Chain),

    %% check the miners nonce values to be sure the txns have actually been absorbed and not just lost
    ExpectedNonce = StartNonce +2,
    true = nonce_updated_for_miner(Addr, ExpectedNonce, ConMiners),

    ok.

txn_out_of_sequence_nonce_test(Config) ->
    %% send two standalone payments, but out of order so that the first submitted has an out of sequence nonce
    %% this will result in validations determining undecided and the txn stays in txn mgr cache
    %% this txn will only clear out after the second txn is submitted
    Miner = hd(?config(non_consensus_miners, Config)),
    ConMiners = ?config(consensus_miners, Config),
    AddrList = ?config(tagged_miner_addresses, Config),

    IgnoredTxns = [],
    Addr = miner_ct_utils:node2addr(Miner, AddrList),

    Chain = ct_rpc:call(Miner, blockchain_worker, blockchain, []),

    PayerAddr = Addr,
    Payee = hd(miner_ct_utils:shuffle(ConMiners)),
    PayeeAddr = miner_ct_utils:node2addr(Payee, AddrList),
    {ok, _Pubkey, SigFun, _ECDHFun} = ct_rpc:call(Miner, blockchain_swarm, keys, []),

    StartNonce = miner_ct_utils:get_nonce(Miner, Addr),
    {ok, _Pubkey, SigFun, _ECDHFun} = ct_rpc:call(Miner, blockchain_swarm, keys, []),

    %% the first txn
    Txn1 = ct_rpc:call(Miner, blockchain_txn_payment_v1, new, [PayerAddr, PayeeAddr, 1000, StartNonce+1]),
    SignedTxn1 = ct_rpc:call(Miner, blockchain_txn_payment_v1, sign, [Txn1, SigFun]),
    %% the second txn
    Txn2 = ct_rpc:call(Miner, blockchain_txn_payment_v1, new, [PayerAddr, PayeeAddr, 1000, StartNonce+2]),
    SignedTxn2 = ct_rpc:call(Miner, blockchain_txn_payment_v1, sign, [Txn2, SigFun]),

    %% send txn 2 first
    ok = ct_rpc:call(Miner, blockchain_worker, submit_txn, [SignedTxn2]),

    %% confirm the txn remains in the txn mgr cache
    %% it should be the only txn
    Result1 = miner_ct_utils:wait_until(
        fun() ->
            case get_cached_txns_with_exclusions(Miner, IgnoredTxns) of
                #{} -> true;
                FilteredTxns ->
                    %% we expect the payment txn to remain as its nonce is too far ahead
                    case FilteredTxns of
                        #{SignedTxn2 := _TxnData} -> true;
                        _ -> false
                    end
            end
        end, 60, 2000),
    ok = handle_get_cached_txn_result(Result1, Miner, IgnoredTxns, Chain),

    %% now submit the other txn which will have the missing nonce
    %% this should result in both this and the previous txn being accepted by the CG
    %% and cleared out of the txn mgr
    ok = ct_rpc:call(Miner, blockchain_worker, submit_txn, [SignedTxn1]),

    %% both txn should now have been accepted by the CG and removed from the txn mgr cache
    %% txn mgr cache should be empty
    Result2 = miner_ct_utils:wait_until(
                                        fun()->
                                            case get_cached_txns_with_exclusions(Miner, IgnoredTxns) of
                                                #{} -> true;
                                                _ -> false
                                            end
                                        end, 60, 2000),
    ok = handle_get_cached_txn_result(Result2, Miner, IgnoredTxns, Chain),

    %% check the miners nonce values to be sure the txns have actually been absorbed and not just lost
    ExpectedNonce = StartNonce +2,
    true = nonce_updated_for_miner(Addr, ExpectedNonce, ConMiners),

    ok.

txn_invalid_nonce_test(Config) ->
    %% send two standalone payments, the second with a duplicate/invalid nonce
    %% the first txn will be successful, the second should be declared invalid
    %% both will be removed from the txn mgr cache,
    %% the first because it is absorbed, the second because it is invalid
    Miner = hd(?config(non_consensus_miners, Config)),
    ConMiners = ?config(consensus_miners, Config),
    AddrList = ?config(tagged_miner_addresses, Config),

    IgnoredTxns = [],
    Addr = miner_ct_utils:node2addr(Miner, AddrList),

    Chain = ct_rpc:call(Miner, blockchain_worker, blockchain, []),

    PayerAddr = Addr,
    Payee = hd(miner_ct_utils:shuffle(ConMiners)),
    PayeeAddr = miner_ct_utils:node2addr(Payee, AddrList),
    {ok, _Pubkey, SigFun, _ECDHFun} = ct_rpc:call(Miner, blockchain_swarm, keys, []),

    StartNonce = miner_ct_utils:get_nonce(Miner, Addr),
    {ok, _Pubkey, SigFun, _ECDHFun} = ct_rpc:call(Miner, blockchain_swarm, keys, []),

    %% the first txn
    Txn1 = ct_rpc:call(Miner, blockchain_txn_payment_v1, new, [PayerAddr, PayeeAddr, 1000, StartNonce+1]),
    SignedTxn1 = ct_rpc:call(Miner, blockchain_txn_payment_v1, sign, [Txn1, SigFun]),
    %% the second txn - with the same nonce as the first
    Txn2 = ct_rpc:call(Miner, blockchain_txn_payment_v1, new, [PayerAddr, PayeeAddr, 1000, StartNonce+1]),
    SignedTxn2 = ct_rpc:call(Miner, blockchain_txn_payment_v1, sign, [Txn2, SigFun]),
    %% send txn1
    ok = ct_rpc:call(Miner, blockchain_worker, submit_txn, [SignedTxn1]),

    %% wait until the first txn has been accepted by the CG and removed from the txn mgr cache
    %% txn mgr cache should be empty
    Result1 = miner_ct_utils:wait_until(
                                        fun()->
                                            case get_cached_txns_with_exclusions(Miner, IgnoredTxns) of
                                                #{} -> true;
                                                _ -> false
                                            end
                                        end, 60, 2000),
    ok = handle_get_cached_txn_result(Result1, Miner, IgnoredTxns, Chain),

    %% now send the second txn ( with the dup nonce )
    ok = ct_rpc:call(Miner, blockchain_worker, submit_txn, [SignedTxn2]),

    %% give the second txn a bit of time to be processed by the txn mgr and for it to be declared invalid
    %% and removed from the txn mgr cache
    Result2 = miner_ct_utils:wait_until(
                                        fun()->
                                            case get_cached_txns_with_exclusions(Miner, IgnoredTxns) of
                                                #{} -> true;
                                                _ -> false
                                            end
                                        end, 60, 2000),
    ok = handle_get_cached_txn_result(Result2, Miner, IgnoredTxns, Chain),

    %% check the miners nonce values to be sure the txns have actually been absorbed and not just lost
    ExpectedNonce = StartNonce +1,
    true = nonce_updated_for_miner(Addr, ExpectedNonce, ConMiners),

    ok.

txn_dependent_test(Config) ->
    %% send a bunch of out of order dependent txns
    %% they should all end up being accepted in the *same* block
    %% but only after the txn with the lowest sequenced nonce is submitted
    %% as until that happens none will pass validations
    %% confirm the 3 txns remain in the txn mgr cache until the missing txn is submitted
    %% after which we confirm the txn mgr cache empties due to the txns being accepted
    %% confirm we dont have any strays in the txn mgr cache at the end of it
    Miners = ?config(miners, Config),
    Miner = hd(?config(non_consensus_miners, Config)),
    ConMiners = ?config(consensus_miners, Config),
    AddrList = ?config(tagged_miner_addresses, Config),

    Addr = miner_ct_utils:node2addr(Miner, AddrList),

    Chain = ct_rpc:call(Miner, blockchain_worker, blockchain, []),

    ct:pal("miner in use ~p", [Miner]),

    PayerAddr = Addr,
    Payee = hd(miner_ct_utils:shuffle(ConMiners)),
    PayeeAddr = miner_ct_utils:node2addr(Payee, AddrList),
    {ok, _Pubkey, SigFun, _ECDHFun} = ct_rpc:call(Miner, blockchain_swarm, keys, []),
    IgnoredTxns = [blockchain_txn_poc_request_v1],
    StartNonce = miner_ct_utils:get_nonce(Miner, Addr),

    %% prep the txns, 1 - 4
    Txn1 = ct_rpc:call(Miner, blockchain_txn_payment_v1, new, [PayerAddr, PayeeAddr, 1000, StartNonce+1]),
    SignedTxn1 = ct_rpc:call(Miner, blockchain_txn_payment_v1, sign, [Txn1, SigFun]),
    Txn2 = ct_rpc:call(Miner, blockchain_txn_payment_v1, new, [PayerAddr, PayeeAddr, 1000, StartNonce+2]),
    SignedTxn2 = ct_rpc:call(Miner, blockchain_txn_payment_v1, sign, [Txn2, SigFun]),
    Txn3 = ct_rpc:call(Miner, blockchain_txn_payment_v1, new, [PayerAddr, PayeeAddr, 1000, StartNonce+3]),
    SignedTxn3 = ct_rpc:call(Miner, blockchain_txn_payment_v1, sign, [Txn3, SigFun]),
    Txn4 = ct_rpc:call(Miner, blockchain_txn_payment_v1, new, [PayerAddr, PayeeAddr, 1000, StartNonce+4]),
    SignedTxn4 = ct_rpc:call(Miner, blockchain_txn_payment_v1, sign, [Txn4, SigFun]),

    %% get the start height
    {ok, Height} = ct_rpc:call(Miner, blockchain, height, [Chain]),

    %% send txns with nonces 2, 3 and 4, all out of sequence
    %% we wont send txn with nonce 1 yet
    %% this means the 3 submitted txns cannot be accepted and will remain in the txn mgr cache
    %% until txn with nonce 1 gets submitted
    ok = ct_rpc:call(Miner, blockchain_worker, submit_txn, [SignedTxn2]),
    ok = ct_rpc:call(Miner, blockchain_worker, submit_txn, [SignedTxn4]),
    ok = ct_rpc:call(Miner, blockchain_worker, submit_txn, [SignedTxn3]),

    %% Wait a few blocks, confirm txns remain in the cache
    ok = miner_ct_utils:wait_for_gte(height_exactly, Miners, Height + 3),

    %% confirm all txns are still be in txn mgr cache
    true = miner_ct_utils:wait_until(
                                        fun()->
                                            maps:size(get_cached_txns_with_exclusions(Miner, IgnoredTxns)) == 3
                                        end, 60, 100),

    %% now submit the remaining txn which will have the missing nonce
    %% this should result in both this and the previous txns being accepted by the CG
    %% and cleared out of the txn mgr cache
    ok = ct_rpc:call(Miner, blockchain_worker, submit_txn, [SignedTxn1]),

    %% Wait one more block
    ok = miner_ct_utils:wait_for_gte(height_exactly, Miners, Height + 4),

    %% confirm all txns are are gone from the cache within the span of a single block
    %% ie they are not carrying across blocks
    true = miner_ct_utils:wait_until(
                                        fun()->
                                            maps:size(get_cached_txns_with_exclusions(Miner, IgnoredTxns)) == 0
                                        end, 60, 100),

    ok = miner_ct_utils:wait_for_gte(height_exactly, Miners, Height + 5),

    ExpectedNonce = StartNonce +4,
    true = nonce_updated_for_miner(Addr, ExpectedNonce, ConMiners),

    ok.



txn_assert_loc_v2_test(Config) ->
    %% send a payment txn, but with an out sequence nonce
    %% this will result in validations determining undecided and the txn will remain in the txn mgr cache
    %% until it has exceeded the max block span of 15 after which it will be declared invalid
    %% and removed from cache
    Miners = ?config(miners, Config),

    ConMiners = ?config(consensus_miners, Config),
    NonConMiners = ?config(non_consensus_miners, Config),
    AddrList = ?config(tagged_miner_addresses, Config),
    Miner = hd(NonConMiners),

    ct:pal("miner in use ~p", [Miner]),

    IgnoredTxns = [blockchain_txn_poc_request_v1],
    Addr = miner_ct_utils:node2addr(Miner, AddrList),

    Chain = ct_rpc:call(Miner, blockchain_worker, blockchain, []),

    _PayerAddr = Addr,
    Payee = hd(miner_ct_utils:shuffle(ConMiners)),
    _PayeeAddr = miner_ct_utils:node2addr(Payee, AddrList),
    {ok, Pubkey, SigFun, _ECDHFun} = ct_rpc:call(Miner, blockchain_swarm, keys, []),
    PubKeyBin = libp2p_crypto:pubkey_to_bin(Pubkey),

    %% get the start height
    {ok, Height} = ct_rpc:call(Miner, blockchain, height, [Chain]),

    %% add the gateway
    #{public := Full2GatewayPubKey, secret := Full2GatewayPrivKey} = libp2p_crypto:generate_keys(ecc_compact),
    Full2Gateway = libp2p_crypto:pubkey_to_bin(Full2GatewayPubKey),
    Full2GatewaySigFun = libp2p_crypto:mk_sig_fun(Full2GatewayPrivKey),
    %% add gateway base txn
    AddFull2GatewayTx0 = ct_rpc:call(Miner, blockchain_txn_add_gateway_v1, new, [PubKeyBin, Full2Gateway, PubKeyBin]),
    AddFull2GatewayTxFee = ct_rpc:call(Miner, blockchain_txn_add_gateway_v1, calculate_fee, [AddFull2GatewayTx0, Chain]),
    AddFull2GatewayStFee = ct_rpc:call(Miner, blockchain_txn_add_gateway_v1, calculate_staking_fee, [AddFull2GatewayTx0, Chain]),

    ct:pal("Add gateway txn fee ~p, staking fee ~p, total: ~p", [AddFull2GatewayTxFee, AddFull2GatewayStFee, AddFull2GatewayTxFee + AddFull2GatewayStFee]),
    %% set the fees on the base txn and then sign the various txns
    AddFull2GatewayTx1 = ct_rpc:call(Miner, blockchain_txn_add_gateway_v1, fee, [AddFull2GatewayTx0, AddFull2GatewayTxFee]),
    AddFull2GatewayTx2 = ct_rpc:call(Miner, blockchain_txn_add_gateway_v1, staking_fee, [AddFull2GatewayTx1, AddFull2GatewayStFee]),
    SignedOwnerAddFull2GatewayTx2 = ct_rpc:call(Miner, blockchain_txn_add_gateway_v1, sign, [AddFull2GatewayTx2, SigFun]),
    SignedGatewayAddFull2GatewayTx2 = ct_rpc:call(Miner, blockchain_txn_add_gateway_v1, sign_request, [SignedOwnerAddFull2GatewayTx2, Full2GatewaySigFun]),
    SignedPayerAddFull2GatewayTx2 = ct_rpc:call(Miner, blockchain_txn_add_gateway_v1, sign_payer, [SignedGatewayAddFull2GatewayTx2, SigFun]),

    ?assertEqual(ok, ct_rpc:call(Miner, blockchain_txn_add_gateway_v1, is_valid, [SignedPayerAddFull2GatewayTx2, Chain])),

    %% submit the add gateway and wait for it to be absorbed
    ok = ct_rpc:call(Miner, blockchain_worker, submit_txn, [SignedPayerAddFull2GatewayTx2]),
    ok = miner_ct_utils:wait_for_gte(height, Miners, Height + 2),

    %% create the assert location v2 txn
    Loc1 = 16#8c1eec6b66517ff,
    {_Txn1, Txn2} = base_assert_loc_v2_txn(Miner, Full2Gateway, PubKeyBin, PubKeyBin, Loc1, 1, Chain),
    ct:pal("assert loc v2 txn 1: ~p", [Txn2]),
    SignedTxn1 = ct_rpc:call(Miner, blockchain_txn_assert_location_v2, sign, [Txn2, SigFun]),
    ct:pal("assert loc v2 SignedTxn1: ~p", [SignedTxn1]),
    SignedTxn2 = ct_rpc:call(Miner, blockchain_txn_assert_location_v2, sign_payer, [SignedTxn1, SigFun]),
    ct:pal("assert loc v2 SignedTxn2: ~p", [SignedTxn2]),
    ?assertEqual(ok, ct_rpc:call(Miner, blockchain_txn_assert_location_v2, is_valid, [SignedTxn2, Chain])),

    %% submit the assert location, wait for it to be absorbed
    ok = ct_rpc:call(Miner, blockchain_worker, submit_txn, [SignedTxn2]),
%%    ok = miner_ct_utils:wait_for_gte(height, Miners, Height + 2),
    ok = ct_rpc:call(Miner, blockchain_worker, submit_txn, [SignedTxn2]),
    ok = miner_ct_utils:wait_for_gte(height, Miners, Height + 4),

%%    Loc2 = 631252734740396943,
%%    {_Txn1b, Txn2b} = base_assert_loc_v2_txn(Miner, Full2Gateway, PubKeyBin, PubKeyBin, Loc2, 2, Chain),
%%    SignedTxn1b = ct_rpc:call(Miner, blockchain_txn_assert_location_v2, sign, [Txn2b, SigFun]),
%%    SignedTxn2b = ct_rpc:call(Miner, blockchain_txn_assert_location_v2, sign_payer, [SignedTxn1b, SigFun]),
%%
%%    {_Txn1c, Txn2c} = base_assert_loc_v2_txn(Miner, Full2Gateway, PubKeyBin, PubKeyBin, Loc2, 2, Chain),
%%    SignedTxn1c = ct_rpc:call(Miner, blockchain_txn_assert_location_v2, sign, [Txn2c, SigFun]),
%%    SignedTxn2c = ct_rpc:call(Miner, blockchain_txn_assert_location_v2, sign_payer, [SignedTxn1c, SigFun]),
%%
%%    ok = ct_rpc:call(Miner, blockchain_worker, submit_txn, [SignedTxn2b]),
%%    ok = ct_rpc:call(Miner, blockchain_worker, submit_txn, [SignedTxn2c]),

    %% wait 4 blocks, confirm the txn is still there
    ok = miner_ct_utils:wait_for_gte(height, Miners, Height + 9),
    true = miner_ct_utils:wait_until(
                                        fun()->
                                            maps:size(get_cached_txns_with_exclusions(Miner, IgnoredTxns)) == 0
                                        end, 60, 200),

    ok.

%% ------------------------------------------------------------------
%% Local Helper functions
%% ------------------------------------------------------------------
handle_get_cached_txn_result(Result, Miner, IgnoredTxns, Chain)->
    case Result of
        true ->
            ok;
        false ->
          TxnList = get_cached_txns_with_exclusions(Miner, IgnoredTxns),
          {ok, CurHeight} = ct_rpc:call(Miner, blockchain, height, [Chain]),
          ct:pal("~p", [miner_ct_utils:format_txn_mgr_list(TxnList)]),
          ct:fail("unexpected txns in txn_mgr cache for miner ~p. Current height ~p",[Miner, CurHeight])
    end.


get_cached_txns_with_exclusions(Miner, Exclusions)->
    case ct_rpc:call(Miner, blockchain_txn_mgr, txn_list, []) of
        TxnMap when map_size(TxnMap) > 0 ->
            ct:pal("~p txns in txn list", [maps:size(TxnMap)]),
            maps:filter(
                fun(Txn, _TxnData)->
                    not lists:member(blockchain_txn:type(Txn), Exclusions) end, TxnMap);
        _ ->
            ct:pal("empty txn map", []),
            #{}
    end.

nonce_updated_for_miner(Addr, ExpectedNonce, ConMiners)->
    true = miner_ct_utils:wait_until(
        fun() ->
            HaveNoncesIncremented =
                lists:map(fun(M) ->
                            Nonce = miner_ct_utils:get_nonce(M, Addr),
                            Nonce == ExpectedNonce
                          end, ConMiners),
            [true] == lists:usort(HaveNoncesIncremented)
        end, 200, 1000).

base_assert_loc_v2_txn(Miner, GatewayPubkeyBin, Owner, Payer, Loc, Nonce, Chain) ->
    Txn0 = ct_rpc:call(Miner, blockchain_txn_assert_location_v2, new, [GatewayPubkeyBin, Owner, Payer, Loc, Nonce]),
    Fee = ct_rpc:call(Miner, blockchain_txn_assert_location_v2, calculate_fee, [Txn0, Chain]),
    SFee = ct_rpc:call(Miner, blockchain_txn_assert_location_v2, calculate_staking_fee, [Txn0, Chain]),
    Txn1 = ct_rpc:call(Miner, blockchain_txn_assert_location_v2, fee, [Txn0, Fee]),
    ct:pal("assert loc v2, txn fee ~p, staking fee ~p, total: ~p", [Fee, SFee, Fee + SFee]),
    {Txn1, ct_rpc:call(Miner, blockchain_txn_assert_location_v2, staking_fee, [Txn1, SFee])}.